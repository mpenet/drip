(ns s-exp.drip.worker
  (:require [clojure.tools.logging :as log]
            [s-exp.drip.client :as client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.duration :as duration])
  (:import (java.time Instant)
           (java.util.concurrent ExecutionException Executors ExecutorService
                                 Future ScheduledExecutorService Semaphore
                                 TimeUnit TimeoutException)))

;; ---------------------------------------------------------------------------
;; Executor internals
;; ---------------------------------------------------------------------------

(defn- make-task-executor ^ExecutorService
  [_concurrency]
  (Executors/newVirtualThreadPerTaskExecutor))

(defn- run-with-timeout
  [^ExecutorService task-executor worker client job timeout-ms]
  (if timeout-ms
    (let [^Future f (.submit task-executor ^Callable #(worker client job))]
      (try
        (.get f (long timeout-ms) TimeUnit/MILLISECONDS)
        (catch TimeoutException _
          (.cancel f true)
          (throw (ex-info "job timed out" {:timeout-ms timeout-ms} (TimeoutException.))))
        (catch ExecutionException e
          (throw (.getCause e)))))
    (worker client job)))

(defn- process-job
  [{:keys [client registry retry-policies job-timeouts
           ^ExecutorService task-executor ^Semaphore semaphore]} job]
  (try
    (let [worker (get registry (:kind job))]
      (if-not worker
        (db/with-tx [tx client]
          (client/discard-job! client tx (:id job)))
        (let [timeout-ms (get job-timeouts (:kind job) (get job-timeouts :default))
              [ok? result-or-ex]
              (try
                [true (run-with-timeout task-executor worker client job timeout-ms)]
                (catch Throwable t
                  [false t]))]
          (if ok?
            nil
            (let [^Throwable t result-or-ex
                  policy (get retry-policies (:kind job) (get retry-policies :default))]
              (db/with-tx [tx client]
                (client/fail-job!
                 client
                 tx
                 (:id job)
                 {:error (or (.getMessage t) (str (class t)))
                  :trace (let [sw (java.io.StringWriter.)
                               pw (java.io.PrintWriter. sw)]
                           (.printStackTrace t pw)
                           (.toString sw))}
                 policy)))))))
    (finally
      (.release semaphore))))

(defn- poll-queue
  [{:keys [client registry retry-policies job-timeouts worker-id concurrency
           ^ExecutorService task-executor ^Semaphore semaphore] :as ctx} q]
  (let [available (.availablePermits semaphore)
        limit (min concurrency available)]
    (when (pos? limit)
      (.acquire semaphore (int limit))
      (let [jobs (db/with-tx [tx client]
                   (when-not (client/queue-paused? client tx q)
                     (client/fetch-jobs! client tx q worker-id {:limit limit})))]
        (if (seq jobs)
          (do
            (when (< (count jobs) limit)
              (.release semaphore (int (- limit (count jobs)))))
            (doseq [job jobs]
              (.submit task-executor
                       ^Runnable (fn []
                                   (process-job ctx job)))))
          (.release semaphore (int limit)))))))

(def default-retention-ms
  {:completed 86400000
   :cancelled 86400000
   :discarded 604800000})

(defn- run-cleaner
  [c tx retention consumed-queues]
  (let [now (Instant/now)
        global (get retention :default)
        per-queue (dissoc retention :default)
        delete! (fn [states-ms qs]
                  (doseq [[state ms] states-ms]
                    (when ms
                      (client/delete-jobs! c tx
                                           (cond-> {:states [state]
                                                    :finalized-before (.minusMillis now (long ms))}
                                             (seq qs) (assoc :queues (vec qs)))))))]
    (doseq [[q-name q-retention] per-queue]
      (delete! (merge global q-retention) [q-name]))
    (when (seq global)
      (let [overridden (set (keys per-queue))
            global-qs (seq (remove overridden consumed-queues))]
        (when (or (empty? overridden) (seq global-qs))
          (delete! global global-qs))))))

(defn- do-poll
  [{:keys [client retry-policies queues rescue-after retention] :as ctx}]
  (try
    (db/with-tx [tx client]
      (client/promote-scheduled-jobs! client tx)
      (when rescue-after
        (let [now (Instant/now)
              default-dur (get rescue-after :default)
              per-queue (dissoc rescue-after :default)
              rescue! (fn [dur qs]
                        (when dur
                          (client/rescue-stuck-jobs! client tx
                                                     (.minusMillis now (long (duration/duration dur)))
                                                     (get retry-policies :default)
                                                     qs)))]
          (doseq [[q-name q-dur] per-queue]
            (rescue! q-dur [q-name]))
          (when default-dur
            (let [overridden (set (keys per-queue))
                  global-qs (seq (remove overridden queues))]
              (when (or (empty? overridden) (seq global-qs))
                (rescue! default-dur global-qs))))))
      (when retention
        (run-cleaner client tx retention queues)))
    (catch Exception t (log/error t "drip: poll maintenance error")))
  (doseq [q queues]
    (try
      (poll-queue ctx q)
      (catch Exception t (log/error t "drip: poll-queue error" {:queue q})))))

;; ---------------------------------------------------------------------------
;; Executor record and lifecycle
;; ---------------------------------------------------------------------------

(defrecord Executor
           [client
            registry
            retry-policies
            job-timeouts
            queues
            concurrency
            poll-interval-ms
            worker-id
            rescue-after
            retention
            ^ExecutorService task-executor
            ^ScheduledExecutorService scheduler
            ^Semaphore semaphore
            listener
            running?])

(defn start-executor!
  "Starts a job executor that polls queues and dispatches jobs to workers.

   Required options:
     :client    - client from s-exp.drip.client.mariadb/make-client (or postgres/sqlite)
     :registry  - {kind-string handler-fn} map
                  Handler receives two args: [client job]
                  Handlers must always explicitly manage job state by calling
                  complete-job!, fail-job!, snooze-job!, etc. Throwing any
                  Throwable signals failure — the worker records the error and
                  schedules a retry (or discards when max attempts is reached).
                  Example:
                    (fn [client job]
                      (drip/with-tx [tx client]
                        (my-business-write! tx (:args job))
                        (drip/complete-job! client tx (:id job))))

   Optional options:
     :queues            - vector of queue names to consume (default [\"default\"])
     :concurrency       - max simultaneous in-flight jobs across all queues (default 10)
     :poll-interval-ms  - polling interval in milliseconds (default 1000)
     :worker-id         - unique string ID (default random UUID)
     :retry-policies    - {kind-string retry-policy-fn, :default retry-policy-fn} map.
                          :default is the fallback policy (default: exponential backoff).
                          kind-string entries override :default for that job kind.
                          Policy fn: (fn [attempt] java.time.Instant)
                          Example: {:default drip/default-retry-policy
                                    \"email\" fast-retry-policy}
     :job-timeouts      - unified timeout config map. :default is the global timeout in ms
                          (nil = no timeout); kind-string keys override per kind.
                          Default: {:default nil} (no timeout).
                          Example: {:default 30000
                                    \"slow_report\" 120000
                                    \"quick_notify\" 5000}
     :rescue-after      - unified rescue config map. :default is the global stuck threshold;
                          queue-name string keys override per queue. Each value is a duration:
                          ms number or duration string (e.g. \"1h\", \"30m\").
                          Set :default to nil to disable global rescue. Set :rescue-after nil
                          to disable rescue entirely.
                          Default: {:default \"1h\"}
                          Example: {:default \"1h\" \"slow\" \"4h\" \"fast\" \"15m\"}
     :retention         - unified retention config map. Keys are either :default (the global
                          {state → ms} map) or queue-name strings (per-queue {state → ms} overrides).
                          Per-queue entries are merged on top of :default for that queue.
                          Set a state to nil to disable cleanup for it. Set :retention nil to
                          disable all automatic cleanup.
                          Default: {:default {:completed 86400000
                                              :cancelled 86400000
                                              :discarded 604800000}}
                          Example: {:default  {:completed 86400000 :discarded 604800000}
                                    \"fast\"    {:completed 3600000}
                                    \"archive\" {:discarded nil}}

   On PostgreSQL, a LISTEN connection is started automatically; inserts from
   other processes trigger an immediate poll instead of waiting for the interval.

   Returns an Executor record. Stop with stop-executor!."
  [{:keys [client registry retry-policies job-timeouts queues
           concurrency poll-interval-ms worker-id rescue-after retention]
    :or {queues ["default"]
         concurrency 10
         poll-interval-ms 1000
         retry-policies {:default job/default-retry-policy}
         job-timeouts {:default nil}
         rescue-after {:default "1h"}
         retention {:default default-retention-ms}}}]
  (let [worker-id (or worker-id (str (random-uuid)))
        task-executor (make-task-executor concurrency)
        scheduler (Executors/newSingleThreadScheduledExecutor)
        semaphore (Semaphore. (int concurrency))
        running? (atom true)
        ctx {:client client
             :registry registry
             :retry-policies retry-policies
             :job-timeouts job-timeouts
             :queues queues
             :worker-id worker-id
             :concurrency concurrency
             :rescue-after rescue-after
             :retention retention
             :task-executor task-executor
             :semaphore semaphore}
        poll-fn (fn []
                  (when @running?
                    (do-poll ctx)))
        listener (client/start-listener! client
                                         (fn [_queue]
                                           (when @running?
                                             (.submit scheduler ^Runnable poll-fn))))]
    (.scheduleWithFixedDelay
     scheduler
     ^Runnable poll-fn
     0
     (long poll-interval-ms)
     TimeUnit/MILLISECONDS)
    (map->Executor
     {:client client
      :registry registry
      :retry-policies retry-policies
      :job-timeouts job-timeouts
      :queues queues
      :concurrency concurrency
      :poll-interval-ms poll-interval-ms
      :worker-id worker-id
      :rescue-after rescue-after
      :retention retention
      :task-executor task-executor
      :scheduler scheduler
      :semaphore semaphore
      :listener listener
      :running? running?})))

(defn stop-executor!
  "Gracefully shuts down the executor.
   Waits up to timeout-ms for in-flight jobs to finish (default 30s).
   Returns true if clean shutdown, false if timed out."
  ([executor]
   (stop-executor! executor 30000))
  ([{:keys [client
            ^ExecutorService task-executor
            ^ScheduledExecutorService scheduler
            listener running?]}
    timeout-ms]
   (reset! running? false)
   (.shutdown scheduler)
   (client/stop-listener! client listener)
   (.shutdown task-executor)
   (.awaitTermination task-executor
                      (long timeout-ms)
                      TimeUnit/MILLISECONDS)))

(defn stop-and-cancel!
  "Immediately cancels all in-flight jobs by interrupting their threads, then shuts down.
   In-flight jobs remain in :running state and will be rescued by rescue-stuck-jobs on the
   next executor startup (or via the periodic rescue in another running executor).
   Returns the list of cancelled Futures from shutdownNow."
  [{:keys [client
           ^ExecutorService task-executor
           ^ScheduledExecutorService scheduler
           listener running?]}]
  (reset! running? false)
  (.shutdown scheduler)
  (client/stop-listener! client listener)
  (.shutdownNow task-executor))
