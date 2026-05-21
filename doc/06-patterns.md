# Patterns and Use Cases

## Transactional enqueue (the core pattern)

Insert a job inside the same transaction as the data it depends on. Both succeed or both fail:

```clojure
(defn create-user! [client user-data]
  (drip/with-tx [tx client]
    (let [user (db/insert-user! tx user-data)]
      (drip/insert-job! client tx "send_welcome_email"
        {:user-id (:id user)
         :email   (:email user)})
      user)))
```

If the transaction rolls back (constraint violation, exception, etc.), the job is never created. If it commits, the job is guaranteed to eventually run. This eliminates the "job queued for an event that never happened" class of bugs.

## Outbox pattern

Complete a job atomically with your own writes. Call `complete-job!` inside your transaction:

```clojure
{"process_payment"
 (fn [client job]
   (drip/with-tx [tx client]
     (let [result (charge-card! tx (:args job))]
       (record-transaction! tx result)
       (drip/record-output! client tx (:id job) {:transaction-id (:id result)})
       (drip/complete-job!  client tx (:id job)))))}
```

If `charge-card!` throws, the transaction rolls back, the job is not completed, and the handler's exception propagates to the worker — which records the error and retries the job. The payment record and the job completion are atomically committed together.

This is the recommended pattern for any handler that writes to the database.

## Fanout: one job spawns many

A single job inserts a batch of child jobs within the same transaction:

```clojure
{"process_batch"
 (fn [client job]
   (let [ids (fetch-pending-ids! (:args job))]
     (drip/with-tx [tx client]
       (drip/insert-many! client tx
         (mapv (fn [id] ["process_item" {:id id} nil]) ids))
       (drip/complete-job! client tx (:id job)))))}
```

The parent job and all child jobs are committed atomically. If the transaction rolls back, none of the child jobs exist.

## Deferred work with snooze

A job can reschedule itself without consuming a retry attempt. Useful for polling or rate-limited work:

```clojure
{"poll_external_api"
 (fn [client job]
   (let [status (check-status (:args job))]
     (case status
       :pending
       (drip/snooze-job client (:id job) "5m")  ; check again in 5 minutes

       :complete
       (do
         (record-result! (:args job))
         (drip/complete-job client (:id job)))

       :failed
       (throw (ex-info "External API failed" {:status status})))))}
```

`snooze-job` reschedules the job to `:scheduled` state without incrementing the attempt counter or consuming retry budget. Useful for:
- Polling external APIs
- Waiting for a lock or resource
- Rate-limited work that must retry later

## Job chaining

One job enqueues the next step on completion:

```clojure
{"step1_extract"
 (fn [client job]
   (let [data (extract! (:args job))]
     (drip/with-tx [tx client]
       (drip/insert-job! client tx "step2_transform" {:data data})
       (drip/complete-job! client tx (:id job)))))}

{"step2_transform"
 (fn [client job]
   (let [result (transform! (:args job))]
     (drip/with-tx [tx client]
       (drip/insert-job! client tx "step3_load" {:result result})
       (drip/complete-job! client tx (:id job)))))}

{"step3_load"
 (fn [client job]
   (load! (:args job))
   (drip/complete-job client (:id job)))}
```

Each step is guaranteed to enqueue the next only if it completes successfully.

## Idempotent handlers

Handlers can run more than once (due to retries or rescued stuck jobs). Design them to be safe when executed multiple times:

```clojure
{"send_email"
 (fn [client job]
   (let [{:keys [user-id email-type]} (:args job)]
     ;; Check if already sent (idempotency key in DB)
     (when-not (email-already-sent? user-id email-type)
       (send-email! user-id email-type)
       (record-email-sent! user-id email-type))
     (drip/complete-job client (:id job))))}
```

Or use a database unique constraint on your business side — the second attempt will fail with a constraint violation which you can catch and treat as success:

```clojure
{"provision_resource"
 (fn [client job]
   (try
     (create-resource! (:args job))
     (catch java.sql.SQLException e
       ;; Duplicate key = already provisioned = success
       (when-not (= "23505" (.getSQLState e))
         (throw e))))
   (drip/complete-job client (:id job)))}
```

## Closing over dependencies

### Via closures

Inject external dependencies via closures:

```clojure
(defn make-registry [email-client stripe-client]
  {"send_email"
   (fn [client job]
     (email/send! email-client (:args job))
     (drip/complete-job client (:id job)))

   "charge_card"
   (fn [client job]
     (drip/with-tx [tx client]
       (let [charge (stripe/charge! stripe-client (:args job))]
         (db/record-charge! tx charge)
         (drip/complete-job! client tx (:id job)))))})

;; Wire up
(def registry (make-registry my-email-client my-stripe-client))
(def worker (drip/start-worker! {:client client :registry registry}))
```

### Via records (Component-friendly)

Define handlers as records implementing `clojure.lang.IFn` via `invoke`. Dependencies are fields injected by Component (or any DI approach):

```clojure
(ns myapp.handlers
  (:require [s-exp.drip :as drip])
  (:gen-class))

(defrecord SendEmailHandler [email-client]
  clojure.lang.IFn
  (invoke [_ client job]
    (email/send! email-client (:args job))
    (drip/complete-job client (:id job))))

(defrecord ChargeCardHandler [stripe-client]
  clojure.lang.IFn
  (invoke [_ client job]
    (drip/with-tx [tx client]
      (let [charge (stripe/charge! stripe-client (:args job))]
        (db/record-charge! tx charge)
        (drip/complete-job! client tx (:id job))))))
```

The registry just maps kind strings to handler instances — they satisfy IFn so the worker calls them like functions:

```clojure
;; In your Component system
(defrecord WorkerRegistry [email-client stripe-client]
  component/Lifecycle
  (start [this]
    (assoc this :registry
      {"send_email"  (->SendEmailHandler email-client)
       "charge_card" (->ChargeCardHandler stripe-client)}))
  (stop [this] this))

(defrecord JobExecutor [drip-client worker-registry]
  component/Lifecycle
  (start [this]
    (assoc this :executor
      (drip/start-worker!
        {:client   drip-client
         :registry (:registry worker-registry)})))
  (stop [{:keys [executor] :as this}]
    (drip/stop-worker! executor :timeout "30s")
    (dissoc this :executor)))
```

This lets you swap handler implementations for testing, inject mocks, and keep handler logic isolated from wiring code.

## Dead-letter handling

Jobs that exhaust their retry budget move to `:discarded`. Set up a periodic job to process them:

```clojure
;; Handler for discarded jobs inspection
{"review_discarded"
 (fn [client _job]
   (let [discarded (drip/list-jobs client {:state :discarded :limit 100})]
     (doseq [job discarded]
       (alert/notify! {:kind   (:kind job)
                       :errors (:errors job)
                       :args   (:args job)}))))}

;; Schedule daily review
(drip/start-periodic-jobs! client
  [{:kind "review_discarded" :args {} :period "24h"}])
```

Or recover specific jobs manually:

```clojure
;; Re-queue a discarded job with updated args
(drip/with-tx [tx client]
  (drip/update-job! client tx job-id {:max-attempts 5})
  (drip/retry-job!  client tx job-id))
```

## Priority-based processing

Use priorities 1–4 (1 = highest) to influence processing order within a queue:

```clojure
;; High-priority user-facing notification
(drip/insert-job client "send_notification" {:user-id 42 :type "password_reset"}
  :priority 1)

;; Low-priority background analytics
(drip/insert-job client "record_event" {:event "page_view" :path "/home"}
  :priority 4)
```

Jobs are fetched ordered by `(priority ASC, scheduled_at ASC)`, so lower-number priorities run first.

## Monitoring job health

```clojure
(defn queue-health [client]
  (let [jobs (drip/list-jobs client {:limit 1000})]
    (group-by :state jobs)))

(defn stuck-job-count [client]
  (let [one-hour-ago (.minusSeconds (Instant/now) 3600)
        running (drip/list-jobs client {:state :running})]
    (count (filter #(.isBefore ^java.time.Instant (:attempted-at %) one-hour-ago)
                   running))))

(defn error-rate [client kind]
  (let [all      (drip/list-jobs client {:kind kind :limit 1000})
        failed   (filter #(= :retryable (:state %)) all)
        discarded (filter #(= :discarded (:state %)) all)]
    {:total    (count all)
     :retrying (count failed)
     :dead     (count discarded)}))
```

## Integration with Stuart Sierra Component

```clojure
(ns myapp.system
  (:require [com.stuartsierra.component :as component]
            [s-exp.drip :as drip]
            [s-exp.drip.client.postgres :as postgres]))

(defrecord DripClient [datasource]
  component/Lifecycle
  (start [this]
    (let [client (doto (postgres/make-client datasource)
                   (drip/migrate!))]
      (assoc this :client client)))
  (stop [this]
    (dissoc this :client)))

(defrecord DripExecutor [drip-client worker-registry queues concurrency]
  component/Lifecycle
  (start [this]
    (assoc this :executor
      (drip/start-worker!
        {:client      (:client drip-client)
         :registry    (:registry worker-registry)
         :queues      (or queues ["default"])
         :concurrency (or concurrency 10)})))
  (stop [{:keys [executor] :as this}]
    (drip/stop-worker! executor :timeout "30s")
    (dissoc this :executor)))

(defn make-system [config]
  (component/system-map
    :datasource    (make-datasource (:db config))
    :drip-client   (component/using (map->DripClient {})
                     [:datasource])
    :worker-registry (component/using (map->WorkerRegistry {})
                       [:email-client :stripe-client])
    :drip-executor (component/using
                     (map->DripExecutor {:queues      ["default"]
                                         :concurrency 20})
                     [:drip-client :worker-registry])))
```

## Testing

### Unit-testing handlers

Handlers are plain functions (or IFn records) — test them directly with a mock client:

```clojure
(deftest email-handler-test
  (let [sent    (atom [])
        client  (reify Object)   ; mock — complete-job is not called in unit test
        handler (->SendEmailHandler (mock-email-client sent))
        job     {:id 1 :kind "send_email" :args {:to "test@example.com"}}]
    ;; call handler directly, bypass executor
    (with-redefs [drip/complete-job (fn [_ _] nil)]
      (handler client job))
    (is (= ["test@example.com"] (map :to @sent)))))
```

For simpler handlers without side effects that matter:

```clojure
(deftest simple-handler-test
  (let [results (atom [])
        handler (fn [client job]
                  (swap! results conj (get-in job [:args :n]))
                  (drip/complete-job client (:id job)))]
    ;; exercise business logic only — no real DB needed
    (with-redefs [drip/complete-job (fn [_ _] nil)]
      (handler nil {:id 1 :args {:n 42}}))
    (is (= [42] @results))))
```

### Integration testing with SQLite

Use an in-memory SQLite database for fast integration tests:

```clojure
(ns myapp.job-test
  (:require [clojure.test :refer :all]
            [next.jdbc.connection :as jdbc]
            [s-exp.drip :as drip]
            [s-exp.drip.client.sqlite :as sqlite])
  (:import (com.zaxxer.hikari HikariDataSource)))

(defmacro with-test-db [[client-sym] & body]
  `(let [ds#     (jdbc/->pool HikariDataSource
                   {:jdbcUrl "jdbc:sqlite::memory:" :maximumPoolSize 1})
         ~client-sym (doto (sqlite/make-client ds#) drip/migrate!)]
     ~@body))

(deftest job-enqueued-and-processed-test
  (with-test-db [client]
    (let [processed (atom [])
          executor  (drip/start-worker!
                      {:client           client
                       :registry         {"work" (fn [_ job]
                                                   (swap! processed conj (:args job)))}
                       :poll-interval 10})]
      (drip/insert-job client "work" {:n 1})
      (drip/insert-job client "work" {:n 2})
      (Thread/sleep 200)
      (drip/stop-worker! executor :timeout "5s")
      (is (= #{1 2} (set (map :n @processed)))))))
```
