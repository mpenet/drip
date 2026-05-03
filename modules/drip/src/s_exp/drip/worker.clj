(ns s-exp.drip.worker
  (:require [clojure.tools.logging :as log]
            [s-exp.drip.client :as client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job])
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
  [c registry retry-policies job-timeouts job default-retry-policy timeout-ms
   ^ExecutorService task-executor ^Semaphore semaphore]
  (try
    (let [worker (get registry (:kind job))]
      (if-not worker
        (db/with-tx [tx c]
          (client/discard-job! c tx (:id job)))
        (let [[ok? result-or-ex]
              (try
                [true (run-with-timeout task-executor worker c job (get job-timeouts (:kind job) timeout-ms))]
                (catch Throwable t
                  [false t]))]
          (if ok?
            nil
            (let [^Throwable t result-or-ex
                  policy (get retry-policies (:kind job) default-retry-policy)]
              (db/with-tx [tx c]
                (client/fail-job!
                 c
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
  [c registry retry-policies job-timeouts timeout-ms q worker-id concurrency
   retry-policy ^ExecutorService task-executor ^Semaphore semaphore]
  (let [available (.availablePermits semaphore)
        limit (min concurrency available)]
    (when (pos? limit)
      (.acquire semaphore (int limit))
      (let [jobs (db/with-tx [tx c]
                   (when-not (client/queue-paused? c tx q)
                     (client/fetch-jobs! c tx q worker-id {:limit limit})))]
        (if (seq jobs)
          (do
            (when (< (count jobs) limit)
              (.release semaphore (int (- limit (count jobs)))))
            (doseq [job jobs]
              (.submit task-executor
                       ^Runnable (fn []
                                   (process-job c registry retry-policies job-timeouts
                                                job retry-policy timeout-ms
                                                task-executor semaphore)))))
          (.release semaphore (int limit)))))))

(def default-retention-ms
  {:completed 86400000
   :cancelled 86400000
   :discarded 604800000})

(defn- run-cleaner
  [c tx retention]
  (let [now (Instant/now)]
    (doseq [[state ms] retention]
      (when ms
        (client/delete-jobs! c
                             tx
                             {:states [state]
                              :finalized-before (.minusMillis now (long ms))})))))

(defn- do-poll
  [c registry retry-policies job-timeouts timeout-ms queues worker-id
   concurrency retry-policy rescue-after retention task-executor
   semaphore]
  (try
    (db/with-tx [tx c]
      (client/promote-scheduled-jobs! c tx)
      (when rescue-after
        (client/rescue-stuck-jobs! c
                                   tx
                                   (.minusMillis ^Instant (Instant/now)
                                                 (long rescue-after))
                                   retry-policy))
      (when retention
        (run-cleaner c tx retention)))
    (catch Exception t (log/error t "drip: poll maintenance error")))
  (doseq [q queues]
    (try
      (poll-queue c
                  registry
                  retry-policies
                  job-timeouts
                  timeout-ms
                  q
                  worker-id
                  concurrency
                  retry-policy
                  task-executor
                  semaphore)
      (catch Exception t (log/error t "drip: poll-queue error" {:queue q})))))

;; ---------------------------------------------------------------------------
;; Executor record and lifecycle
;; ---------------------------------------------------------------------------

(defrecord Executor
           [client
            registry
            retry-policies
            job-timeouts
            timeout-ms
            queues
            concurrency
            poll-interval-ms
            worker-id
            retry-policy
            rescue-after-ms
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
     :retry-policy      - default retry policy fn: (fn [attempt] java.time.Instant) (default exponential backoff)
     :retry-policies    - {kind-string retry-policy-fn} map for per-kind overrides (default {})
     :timeout-ms        - global job execution timeout in ms; nil = no timeout (default nil)
     :job-timeouts      - {kind-string timeout-ms} map for per-kind overrides (default {})
     :rescue-after-ms   - rescue jobs stuck in :running longer than this many ms (default 3600000 = 1h)
                          Set to nil to disable rescue.
     :retention         - map of state keyword → retention-ms; jobs finalized longer ago are deleted.
                          Defaults to {:completed 86400000 :cancelled 86400000 :discarded 604800000}.
                          Set to nil to disable automatic cleanup.

   On PostgreSQL, a LISTEN connection is started automatically; inserts from
   other processes trigger an immediate poll instead of waiting for the interval.

   Returns an Executor record. Stop with stop-executor!."
  [{:keys [client registry retry-policies job-timeouts timeout-ms queues
           concurrency poll-interval-ms worker-id retry-policy rescue-after-ms
           retention]
    :or {queues ["default"]
         concurrency 10
         poll-interval-ms 1000
         retry-policy job/default-retry-policy
         retry-policies {}
         job-timeouts {}
         rescue-after-ms 3600000
         retention default-retention-ms}}]
  (let [worker-id (or worker-id (str (random-uuid)))
        task-executor (make-task-executor concurrency)
        scheduler (Executors/newSingleThreadScheduledExecutor)
        semaphore (Semaphore. (int concurrency))
        running? (atom true)
        poll-fn (fn []
                  (when @running?
                    (do-poll client registry retry-policies job-timeouts
                             timeout-ms queues worker-id concurrency
                             retry-policy rescue-after-ms retention
                             task-executor semaphore)))
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
      :timeout-ms timeout-ms
      :queues queues
      :concurrency concurrency
      :poll-interval-ms poll-interval-ms
      :worker-id worker-id
      :retry-policy retry-policy
      :rescue-after-ms rescue-after-ms
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
