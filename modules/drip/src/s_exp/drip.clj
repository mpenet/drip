(ns s-exp.drip
  "Drip: a Clojure job queue for MariaDB, PostgreSQL, and SQLite.

   Quick start:

     (require '[s-exp.drip :as drip]
              '[s-exp.drip.client.mariadb :as mariadb])
     ;; or: '[s-exp.drip.client.postgres :as postgres]
     ;; or: '[s-exp.drip.client.sqlite   :as sqlite]

     ;; 1. Create a client
     (def client (mariadb/make-client datasource))

     ;; 2. Run migrations (idempotent)
     (drip/migrate! client)

     ;; 3. Start the executor (registry maps kind strings to (fn [client job] ...) fns)
     (def executor
       (drip/start-executor!
         {:client client
          :registry {\"send_email\" (fn [_ job] (send-email! (:args job)))}
          :queues [\"default\" \"priority\"]}))

     ;; 4. Insert jobs
     (drip/insert-job client \"send_email\" {:to \"user@example.com\"})

     ;; 5. Stop on shutdown
     (drip/stop-executor! executor)"
  (:require [s-exp.drip.client :as client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.drip.periodic :as periodic]
            [s-exp.drip.worker :as worker]))

;; ---------------------------------------------------------------------------
;; Transaction helper
;; ---------------------------------------------------------------------------

(defmacro with-tx
  "Opens a transaction from client and binds it to tx-sym, passing remaining
   args to next.jdbc/with-transaction. Body executes within the transaction.

   Example:
     (drip/with-tx [tx client]
       (drip/insert-job! client tx \"k\" {} nil)
       (my-business-write! tx data))"
  [[tx-sym client & jdbc-opts] & body]
  `(db/with-tx [~tx-sym ~client ~@jdbc-opts] ~@body))

;; ---------------------------------------------------------------------------
;; Schema
;; ---------------------------------------------------------------------------

(defn migrate!
  "Creates all drip_* tables and indexes. Idempotent - safe to call on startup."
  [client]
  (db/migrate! client))

;; ---------------------------------------------------------------------------
;; Job operations
;; ---------------------------------------------------------------------------

(defn insert-job
  "Inserts a job using the client's own datasource. Returns the created Job record.
   `kind` - string job type identifier
   `args` - map of job arguments (JSON-serializable)
   `opts` - insert opts as keyword args or a map, e.g.:
     (insert-job client \"k\" {} :queue \"bulk\" :priority 2)
     (insert-job client \"k\" {} {:queue \"bulk\" :priority 2})"
  [client kind args & {:as opts}]
  (let [job (with-tx [tx client]
              (client/insert-job! client tx kind args opts))]
    (when job (client/notify-job-available! client (:queue job)))
    job))

(defn insert-job!
  "Inserts a job using the supplied tx. Returns the created Job record.
   `kind` - string job type identifier
   `args` - map of job arguments (JSON-serializable)
   `opts` - insert opts as keyword args or a map, e.g.:
     (insert-job! client tx \"k\" {} :queue \"bulk\" :priority 2)
     (insert-job! client tx \"k\" {} {:queue \"bulk\" :priority 2})"
  [client tx kind args & {:as opts}]
  (client/insert-job! client tx kind args opts))

(defn insert-many
  "Inserts multiple jobs in a single transaction using the client's own datasource.
   `job-specs` - sequence of [kind args opts] tuples.
   Returns a vector of Job records."
  [client job-specs]
  (let [jobs (with-tx [tx client]
               (mapv (fn [[kind args opts]] (client/insert-job! client tx kind args opts))
                     job-specs))]
    (doseq [job jobs]
      (when job (client/notify-job-available! client (:queue job))))
    jobs))

(defn insert-many!
  "Inserts multiple jobs in a single transaction using the supplied tx.
   `job-specs` - sequence of [kind args opts] tuples.
   Returns a vector of Job records."
  [client tx job-specs]
  (mapv (fn [[kind args opts]] (client/insert-job! client tx kind args opts))
        job-specs))

(defn insert-many-fast!
  "PostgreSQL only. High-throughput batch insert using COPY FROM STDIN.
   `conn` must be a raw java.sql.Connection — obtain one from the DataSource directly:
     (with-open [conn (.getConnection (:ds client))]
       (drip/insert-many-fast! conn job-specs))
   `job-specs` - sequence of [kind args opts] tuples.
   Returns the number of rows inserted (long).
   Limitations vs insert-many:
     - Does not support :unique-opts deduplication
     - Does not return Job records
     - PostgreSQL only (uses org.postgresql.copy.CopyManager)"
  [conn job-specs]
  (let [pg-ns 's-exp.drip.client.postgres]
    (require pg-ns)
    ((ns-resolve (find-ns pg-ns) 'insert-many-fast!) conn job-specs)))

(defn fetch-jobs
  "Atomically claims up to limit available jobs from queue.
   Opens its own transaction. Returns vector of Job records.
   opts: :limit (default 10)"
  [client queue worker-id & {:as opts}]
  (with-tx [tx client]
    (client/fetch-jobs! client tx queue worker-id opts)))

(defn fetch-jobs!
  "Atomically claims up to limit available jobs from queue using the supplied tx.
   Returns vector of Job records.
   opts: :limit (default 10)"
  [client tx queue worker-id opts]
  (client/fetch-jobs! client tx queue worker-id opts))

(defn get-job
  "Returns a Job record by ID using the client's own datasource, or nil if not found."
  [client job-id]
  (with-tx [tx client]
    (client/get-job! client tx job-id)))

(defn get-job!
  "Returns a Job record by ID using the supplied tx, or nil if not found."
  [client tx job-id]
  (client/get-job! client tx job-id))

(defn list-jobs
  "Lists jobs with optional filters. Uses the client's own datasource.
   opts keys:
     :state            - keyword state filter (single)
     :states           - coll of state keywords (multi-value OR)
     :kind             - string kind filter (single)
     :kinds            - coll of kind strings (multi-value OR)
     :queue            - string queue filter (single)
     :queues           - coll of queue names (multi-value OR)
     :priorities       - coll of priority ints (multi-value OR)
     :created-after    - java.time.Instant lower bound on created_at (inclusive)
     :created-before   - java.time.Instant upper bound on created_at (inclusive)
     :scheduled-after  - java.time.Instant lower bound on scheduled_at (inclusive)
     :scheduled-before - java.time.Instant upper bound on scheduled_at (inclusive)
     :limit            - max rows (default 100)
     :after            - job ID cursor for pagination; returns jobs with id < after"
  [client opts]
  (with-tx [tx client]
    (client/list-jobs! client tx opts)))

(defn list-jobs!
  "Lists jobs with optional filters. Uses the supplied tx.
   See list-jobs for opts documentation."
  [client tx opts]
  (client/list-jobs! client tx opts))

(defn update-job
  "Updates writable fields of a job. Uses the client's own datasource. Returns updated Job record.
   opts keys (all optional — only provided keys are applied):
     :metadata     - map; replaces job metadata
     :priority     - integer 1-4
     :queue        - string queue name
     :scheduled-at - java.time.Instant; also transitions state to :scheduled (future) or :available (past)
     :state        - keyword; only :available :scheduled :cancelled :discarded allowed
     :max-attempts - integer
     :tags         - vector of strings"
  [client job-id opts]
  (with-tx [tx client]
    (client/update-job! client tx job-id opts)))

(defn update-job!
  "Updates writable fields of a job. Uses the supplied tx. Returns updated Job record.
   See update-job for opts documentation."
  [client tx job-id opts]
  (client/update-job! client tx job-id opts))

(defn swap-job!
  "Fetches a job by ID within the supplied tx, applies f to it, and updates
   writable fields from the returned map. f receives the Job record and must
   return a map with any subset of update-job keys:
     :metadata :priority :queue :scheduled-at :state :max-attempts :tags
   Returns the updated Job record, or nil if not found."
  [client tx job-id f]
  (when-let [job (client/get-job! client tx job-id)]
    (client/update-job! client tx job-id (f job))))

(defn swap-job
  "Fetches a job by ID, applies f to it, and updates writable fields from the
   returned map. f receives the Job record and must return a map with any subset
   of update-job keys:
     :metadata :priority :queue :scheduled-at :state :max-attempts :tags
   Uses the client's own datasource. Returns the updated Job record, or nil if not found."
  [client job-id f]
  (with-tx [tx client]
    (swap-job! client tx job-id f)))

(defn record-output
  "Merges {:output output} into the job's metadata column.
   Uses the client's own datasource. Returns updated Job record.
   Output is accessible as (get-in job [:metadata \"output\"]) after completion."
  [client job-id output]
  (with-tx [tx client]
    (client/record-output! client tx job-id output)))

(defn record-output!
  "Merges {:output output} into the job's metadata column. Uses the supplied tx.
   Returns updated Job record."
  [client tx job-id output]
  (client/record-output! client tx job-id output))

(defn complete-job
  "Marks a job as :completed. Uses the client's own datasource. Returns updated Job record."
  [client job-id]
  (with-tx [tx client]
    (client/complete-job! client tx job-id)))

(defn complete-job!
  "Marks a job as :completed. Uses the supplied tx. Returns updated Job record."
  [client tx job-id]
  (client/complete-job! client tx job-id))

(defn cancel-job
  "Cancels a job that is not already finalized. Uses the client's own datasource."
  [client job-id]
  (with-tx [tx client]
    (client/cancel-job! client tx job-id)))

(defn cancel-job!
  "Cancels a job that is not already finalized. Uses the supplied tx."
  [client tx job-id]
  (client/cancel-job! client tx job-id))

(defn retry-job
  "Forces a failed/cancelled/discarded job back to :available.
   Uses the client's own datasource."
  [client job-id]
  (with-tx [tx client]
    (client/retry-job! client tx job-id)))

(defn retry-job!
  "Forces a failed/cancelled/discarded job back to :available. Uses the supplied tx."
  [client tx job-id]
  (client/retry-job! client tx job-id))

(defn discard-job
  "Moves a job to :discarded state. Uses the client's own datasource."
  [client job-id]
  (with-tx [tx client]
    (client/discard-job! client tx job-id)))

(defn discard-job!
  "Moves a job to :discarded state. Uses the supplied tx."
  [client tx job-id]
  (client/discard-job! client tx job-id))

(defn delete-job
  "Hard-deletes a job by ID regardless of state. Uses the client's own datasource.
   Returns the deleted Job record, or nil if not found."
  [client job-id]
  (with-tx [tx client]
    (client/delete-job! client tx job-id)))

(defn delete-job!
  "Hard-deletes a job by ID regardless of state. Uses the supplied tx.
   Returns the deleted Job record, or nil if not found."
  [client tx job-id]
  (client/delete-job! client tx job-id))

(defn snooze-job
  "Reschedules a :running job to run again after duration without consuming a retry attempt.
   duration - ms number or duration string (e.g. \"1h\", \"30m\", \"90s\").
   Uses the client's own datasource. Returns updated Job record."
  [client job-id duration]
  (with-tx [tx client]
    (client/snooze-job! client tx job-id duration)))

(defn snooze-job!
  "Reschedules a :running job to run again after duration without consuming a retry attempt.
   duration - ms number or duration string (e.g. \"1h\", \"30m\", \"90s\").
   Uses the supplied tx. Returns updated Job record."
  [client tx job-id duration]
  (client/snooze-job! client tx job-id duration))

(defn rescue-stuck-jobs
  "Rescues jobs stuck in :running state with attempted_at <= stuck-after.
   Transitions each to :retryable or :discarded (based on attempt vs max-attempts).
   Uses the client's own datasource. Returns count of rescued jobs.
   queues - optional seq of queue names to restrict rescue to; nil = all queues."
  ([client stuck-after retry-policy]
   (rescue-stuck-jobs client stuck-after retry-policy nil))
  ([client stuck-after retry-policy queues]
   (with-tx [tx client]
     (client/rescue-stuck-jobs! client tx stuck-after retry-policy queues))))

(defn rescue-stuck-jobs!
  "Rescues jobs stuck in :running state with attempted_at <= stuck-after.
   Uses the supplied tx. Returns count of rescued jobs.
   queues - optional seq of queue names to restrict rescue to; nil = all queues."
  ([client tx stuck-after retry-policy]
   (client/rescue-stuck-jobs! client tx stuck-after retry-policy nil))
  ([client tx stuck-after retry-policy queues]
   (client/rescue-stuck-jobs! client tx stuck-after retry-policy queues)))

(defn delete-jobs
  "Deletes jobs matching opts. Uses the client's own datasource.
   Returns count of deleted jobs.
   opts keys:
     :states           - set/seq of state keywords (required)
     :kinds            - coll of kind strings
     :queues           - coll of queue names
     :priorities       - coll of priority ints
     :finalized-before - java.time.Instant; only delete with finalized_at <= this
     :created-before   - java.time.Instant; only delete with created_at <= this"
  [client opts]
  (with-tx [tx client]
    (client/delete-jobs! client tx opts)))

(defn delete-jobs!
  "Deletes jobs matching opts. Uses the supplied tx.
   Returns count of deleted jobs. See delete-jobs for opts documentation."
  [client tx opts]
  (client/delete-jobs! client tx opts))

;; ---------------------------------------------------------------------------
;; Queue operations
;; ---------------------------------------------------------------------------

(defn upsert-queue
  "Creates or updates a queue. Uses the client's own datasource."
  [client queue-name metadata]
  (with-tx [tx client]
    (client/upsert-queue! client tx queue-name metadata)))

(defn upsert-queue!
  "Creates or updates a queue. Uses the supplied tx."
  [client tx queue-name metadata]
  (client/upsert-queue! client tx queue-name metadata))

(defn pause-queue
  "Pauses a queue - workers stop fetching from it. Uses the client's own datasource."
  [client queue-name]
  (with-tx [tx client]
    (client/pause-queue! client tx queue-name)))

(defn pause-queue!
  "Pauses a queue - workers stop fetching from it. Uses the supplied tx."
  [client tx queue-name]
  (client/pause-queue! client tx queue-name))

(defn resume-queue
  "Resumes a paused queue. Uses the client's own datasource."
  [client queue-name]
  (with-tx [tx client]
    (client/resume-queue! client tx queue-name)))

(defn resume-queue!
  "Resumes a paused queue. Uses the supplied tx."
  [client tx queue-name]
  (client/resume-queue! client tx queue-name))

(defn list-queues
  "Returns all queues. Uses the client's own datasource."
  [client]
  (with-tx [tx client]
    (client/list-queues! client tx)))

(defn list-queues!
  "Returns all queues. Uses the supplied tx."
  [client tx]
  (client/list-queues! client tx))

;; ---------------------------------------------------------------------------
;; Worker / Executor
;; ---------------------------------------------------------------------------

(defn start-executor!
  "Starts polling queues and processing jobs.
   :client and :registry are required. See s-exp.drip.worker/start-executor! for full option docs.

   Registry handlers receive [client job] as two arguments.
   Handlers must explicitly call complete-job!, snooze-job!, etc. to manage job state.
   Throwing any Throwable signals failure and triggers retry or discard.

   Key options:
     :retry-policies - {:default policy-fn, \"kind\" policy-fn} unified retry map
     :job-timeouts   - {:default timeout-ms, \"kind\" timeout-ms} unified timeout map (nil = no timeout)
     :rescue-after   - {:default duration, \"queue\" duration} unified rescue map (nil or :default nil = disable)
     :retention      - {:default {state → ms}, \"queue\" {state → ms}} unified retention map
                       Set :retention nil to disable all cleanup."
  [opts]
  (worker/start-executor! opts))

(defn stop-executor!
  "Gracefully stops the executor. Optional second arg: timeout-ms (default 30000)."
  ([executor]
   (worker/stop-executor! executor))
  ([executor timeout-ms]
   (worker/stop-executor! executor timeout-ms)))

(defn stop-and-cancel!
  "Immediately interrupts all in-flight jobs and shuts down the executor.
   In-flight jobs remain in :running state; rescue-stuck-jobs will requeue them.
   Use stop-executor! instead when you want to wait for jobs to finish gracefully."
  [executor]
  (worker/stop-and-cancel! executor))

;; ---------------------------------------------------------------------------
;; Periodic jobs
;; ---------------------------------------------------------------------------

(defn start-periodic-executor!
  "Schedules periodic job insertions. `specs` is a sequence of PeriodicSpec maps.
   Returns a scheduler; stop with stop-periodic-executor!."
  [client specs]
  (periodic/start-periodic-executor! client specs))

(defn stop-periodic-executor!
  "Stops the periodic job scheduler."
  [scheduler]
  (periodic/stop-periodic-executor! scheduler))

;; ---------------------------------------------------------------------------
;; Retry policy
;; ---------------------------------------------------------------------------

(def default-retry-policy
  "Default retry policy fn. Takes attempt (1-based long), returns java.time.Instant.
   Uses exponential backoff: attempt^4 seconds ± 10% jitter."
  job/default-retry-policy)

(def constant-retry-policy
  "Returns a retry policy fn that always waits `delay` between retries.
   `delay` is a duration value: string (e.g. \"30s\", \"2m\") or number of milliseconds.
   Options:
     :jitter - fractional ± jitter applied to delay (default 0.0)
   Example: (constant-retry-policy \"30s\" :jitter 0.1)"
  job/constant-retry-policy)

(def linear-retry-policy
  "Returns a retry policy fn that waits `base` * attempt.
   `base` is a duration value: string (e.g. \"10s\") or number of milliseconds.
   Options:
     :max    - duration cap on computed delay (default unbounded)
     :jitter - fractional ± jitter applied to delay (default 0.0)
   Example: (linear-retry-policy \"10s\" :max \"5m\" :jitter 0.1)"
  job/linear-retry-policy)

(def exponential-retry-policy
  "Returns a retry policy fn with configurable exponential backoff.
   Waits `base` * multiplier^(attempt-1), capped at `max`.
   `base` is a duration value: string (e.g. \"1s\") or number of milliseconds.
   Options:
     :multiplier - growth factor (default 2.0)
     :max        - duration cap on computed delay (default \"1h\")
     :jitter     - fractional ± jitter applied to delay (default 0.1)
   Example: (exponential-retry-policy \"1s\" :multiplier 2.0 :max \"30m\" :jitter 0.15)"
  job/exponential-retry-policy)

(def immediate-retry-policy
  "Returns a retry policy fn that retries immediately with no delay.
   Useful for tests or jobs that should be reattempted without waiting.
   (immediate-retry-policy)"
  job/immediate-retry-policy)

(def default-retention-ms worker/default-retention-ms)

