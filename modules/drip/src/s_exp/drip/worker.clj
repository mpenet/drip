(ns s-exp.drip.worker
  (:require [clojure.tools.logging :as log]
            [s-exp.drip.client :as client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.duration :as duration])
  (:import (java.util.concurrent ExecutionException Executors ExecutorService
                                 Future ScheduledExecutorService Semaphore
                                 TimeUnit TimeoutException)))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; Executor internals
;; ---------------------------------------------------------------------------

(defn- make-task-executor ^ExecutorService
  [_concurrency]
  (Executors/newVirtualThreadPerTaskExecutor))

(defn- emit! [event-fn event]
  (when event-fn
    (try (event-fn event) (catch Exception _))))

(defn- run-with-timeout
  [^ExecutorService task-executor worker client job timeout-ms]
  (if timeout-ms
    (let [^Future f (.submit task-executor ^Callable #(worker client job))]
      (try
        (.get f (long timeout-ms) TimeUnit/MILLISECONDS)
        (catch TimeoutException _
          (.cancel f true)
          (throw (ex-info "job timed out" {:timeout timeout-ms} (TimeoutException.))))
        (catch ExecutionException e
          (throw (.getCause e)))))
    (worker client job)))

(defn- timeout-ex? [^Throwable t]
  (instance? TimeoutException (.getCause t)))

(defn- process-job
  [{:keys [client registry retry-policies job-timeouts event-fn worker-id
           ^ExecutorService task-executor ^Semaphore semaphore]} job]
  (let [base {:worker-id worker-id
              :queue (:queue job)
              :kind (:kind job)
              :job-id (:id job)
              :attempt (:attempt job)}]
    (try
      (let [handler (get registry (:kind job))]
        (if-not handler
          (do (db/with-tx [tx client]
                (client/discard-job! client tx (:id job)))
              (emit! event-fn (assoc base :type :s-exp.drip.job/discard)))
          (let [timeout-ms (some-> (get job-timeouts (:kind job) (get job-timeouts :default))
                                   duration/duration
                                   long)
                t0 (System/currentTimeMillis)
                _ (emit! event-fn (assoc base :type :s-exp.drip.job/start))
                [ok? result-or-ex]
                (try
                  [true (run-with-timeout task-executor handler client job timeout-ms)]
                  (catch Throwable t
                    [false t]))]
            (let [duration-ms (- (System/currentTimeMillis) t0)]
              (if ok?
                (emit! event-fn (assoc base :type :s-exp.drip.job/complete :duration-ms duration-ms))
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
                     policy))
                  (emit! event-fn (assoc base
                                         :type (if (timeout-ex? t)
                                                 :s-exp.drip.job/timeout
                                                 :s-exp.drip.job/fail)
                                         :duration-ms duration-ms
                                         :error t))))))))
      (finally
        (.release semaphore)))))

(defn- poll-queue
  [{:keys [client worker-id concurrency event-fn
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
            (emit! event-fn {:type :s-exp.drip.poll/fetched
                             :worker-id worker-id
                             :queue q
                             :count (count jobs)})
            (doseq [job jobs]
              (.submit task-executor
                       ^Runnable (fn []
                                   (process-job ctx job)))))
          (.release semaphore (int limit)))))))

(defn- do-poll [{:keys [client queues] :as ctx}]
  (try
    (db/with-tx [tx client]
      (client/promote-scheduled-jobs! client tx))
    (catch Exception t (log/error t "drip: poll maintenance error")))
  (doseq [q queues]
    (try
      (poll-queue ctx q)
      (catch Exception t (log/error t "drip: poll-queue error" {:queue q})))))

;; ---------------------------------------------------------------------------
;; Worker record and lifecycle
;; ---------------------------------------------------------------------------

(defrecord Worker
           [client
            registry
            retry-policies
            job-timeouts
            queues
            concurrency
            poll-interval
            worker-id
            event-fn
            ^ExecutorService task-executor
            ^ScheduledExecutorService scheduler
            ^Semaphore semaphore
            listener
            running?])

(defn start-worker!
  "Starts a job worker that polls queues and dispatches jobs to handlers.

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
     :poll-interval     - polling interval; duration string or ms (default \"1s\")
     :worker-id         - unique string ID (default random UUID)
     :retry-policies    - {kind-string retry-policy-fn, :default retry-policy-fn} map.
                          :default is the fallback policy (default: exponential backoff).
                          kind-string entries override :default for that job kind.
                          Policy fn: (fn [attempt] java.time.Instant)
                          Example: {:default drip/default-retry-policy
                                    \"email\" fast-retry-policy}
     :job-timeouts      - unified timeout config map. :default is the global timeout
                          as a duration string or ms (nil = no timeout);
                          kind-string keys override per kind.
                          Default: {:default nil} (no timeout).
                          Example: {:default \"30s\"
                                    \"slow_report\" \"2m\"
                                    \"quick_notify\" \"5s\"}
     :event-fn          - optional (fn [event]) called for each worker event.
                          Exceptions thrown by the fn are swallowed.
                          Event map keys: :type :worker-id :queue :kind :job-id
                                          :attempt :duration-ms :error :count
                          Event types:
                            :s-exp.drip.job/start    - job dispatched to handler
                            :s-exp.drip.job/complete - handler returned without error
                            :s-exp.drip.job/fail     - handler threw an exception
                            :s-exp.drip.job/timeout  - job exceeded its timeout
                            :s-exp.drip.job/discard  - no handler registered for kind
                            :s-exp.drip.poll/fetched - jobs claimed from a queue

   On PostgreSQL, a LISTEN connection is started automatically; inserts from
   other processes trigger an immediate poll instead of waiting for the interval.

   Returns a Worker record. Stop with stop-worker!."
  [{:keys [client registry retry-policies job-timeouts queues
           concurrency poll-interval worker-id event-fn]
    :or {queues ["default"]
         concurrency 10
         poll-interval "1s"
         retry-policies {:default job/default-retry-policy}
         job-timeouts {:default nil}}}]
  (let [worker-id (or worker-id (str (random-uuid)))
        poll-interval-ms (long (duration/duration poll-interval))
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
             :event-fn event-fn
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
     poll-interval-ms
     TimeUnit/MILLISECONDS)
    (map->Worker
     {:client client
      :registry registry
      :retry-policies retry-policies
      :job-timeouts job-timeouts
      :queues queues
      :concurrency concurrency
      :poll-interval poll-interval
      :worker-id worker-id
      :event-fn event-fn
      :task-executor task-executor
      :scheduler scheduler
      :semaphore semaphore
      :listener listener
      :running? running?})))

(defn stop-worker!
  "Gracefully shuts down the worker.

   Options (keyword args):
     :timeout - max time to wait for in-flight jobs. Duration string or ms (default \"30s\")
     :drain   - when true, stop polling immediately but wait for all
                in-flight jobs to finish naturally before shutting down
                the executor. Uses :timeout as the drain budget.
                Useful for rolling deploys.

   Returns true if clean shutdown within timeout, false if timed out."
  [{:keys [client
           ^ExecutorService task-executor
           ^ScheduledExecutorService scheduler
           ^Semaphore semaphore
           concurrency
           listener running?]}
   & {:keys [timeout drain]
      :or {timeout "30s"}}]
  (reset! running? false)
  (.shutdown scheduler)
  (client/stop-listener! client listener)
  (let [deadline (+ (System/currentTimeMillis) (long (duration/duration timeout)))]
    (when drain
      (let [remaining (max 0 (- deadline (System/currentTimeMillis)))]
        (when (.tryAcquire semaphore (int concurrency) remaining TimeUnit/MILLISECONDS)
          (.release semaphore (int concurrency)))))
    (.shutdown task-executor)
    (let [remaining (max 0 (- deadline (System/currentTimeMillis)))]
      (.awaitTermination task-executor remaining TimeUnit/MILLISECONDS))))

(defn stop-and-cancel!
  "Immediately cancels all in-flight jobs by interrupting their threads, then shuts down.
   In-flight jobs remain in :running state and will be rescued by rescue-stuck-jobs on the
   next worker startup (or via the periodic rescue in another running worker).
   Returns the list of cancelled Futures from shutdownNow."
  [{:keys [client
           ^ExecutorService task-executor
           ^ScheduledExecutorService scheduler
           listener running?]}]
  (reset! running? false)
  (.shutdown scheduler)
  (client/stop-listener! client listener)
  (.shutdownNow task-executor))
