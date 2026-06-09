(ns s-exp.drip.client)

;; ---------------------------------------------------------------------------
;; Protocols
;;
;; A client record implements all four protocols. Use one of the constructors:
;;   s-exp.drip.client.mariadb/make-client
;;   s-exp.drip.client.postgres/make-client
;;   s-exp.drip.client.sqlite/make-client
;; ---------------------------------------------------------------------------

(defprotocol Migration
  (migration-config [client]
    "Returns a migratus config map (without :store and :db) for this client."))

(defprotocol Notifications
  (notify-job-available! [client queue]
    "Notifies listeners that jobs are available in queue.
     No-op on MariaDB/SQLite; calls pg_notify on PostgreSQL.")
  (start-listener! [client on-notify]
    "Starts background LISTEN connection, calls (on-notify queue) on each
     notification. Returns opaque listener handle (nil if unsupported).")
  (stop-listener! [client listener]
    "Stops listener returned by start-listener!. No-op if listener is nil."))

(defprotocol Jobs
  (insert-job! [client tx kind args opts]
    "Inserts a job using tx. Returns a Job record.")
  (fetch-jobs! [client tx queue worker-id opts]
    "Atomically claims up to limit available jobs using the supplied tx.
     opts: :limit (default 10). Returns vector of Job records.")
  (record-output! [client tx job-id output]
    "Merges {:output output} into the job's metadata column. Returns updated Job record.
     Output is accessible as (get-in job [:metadata \"output\"]) after completion.")
  (complete-job! [client tx job-id]
    "Marks job as :completed. Returns updated Job record.")
  (fail-job! [client tx job-id error-map retry-policy]
    "Records error, transitions to :retryable or :discarded. Returns updated Job record.")
  (cancel-job! [client tx job-id]
    "Cancels a non-finalized job. Returns updated Job record.")
  (retry-job! [client tx job-id]
    "Forces job back to :available. Returns updated Job record.")
  (discard-job! [client tx job-id]
    "Moves job to :discarded. Returns updated Job record.")
  (snooze-job! [client tx job-id duration]
    "Reschedules a :running job to run again after duration (ms number or duration string e.g. \"1h\")
     without consuming a retry attempt. Returns updated Job record.")
  (promote-scheduled-jobs! [client tx]
    "Moves :scheduled/:retryable jobs whose scheduled_at <= now to :available.
     Returns count of promoted jobs.")
  (rescue-stuck-jobs! [client tx stuck-after retry-policy queues]
    "Moves :running jobs with attempted_at <= stuck-after to :retryable or :discarded.
     Appends a rescue error entry to each job. Returns count of rescued jobs.
     queues - optional seq of queue names to restrict rescue to; nil = all queues.")
  (delete-jobs! [client tx opts]
    "Deletes jobs matching opts. Returns count of deleted jobs.
     opts keys:
       :states           - set/seq of state keywords (required)
       :kinds            - coll of kind strings
       :queues           - coll of queue names
       :priorities       - coll of priority ints
       :finalized-before - java.time.Instant; only delete with finalized_at <= this
       :created-before   - java.time.Instant; only delete with created_at <= this")
  (update-job! [client tx job-id opts]
    "Updates writable fields of a job. Returns updated Job record, or nil if not found.
     opts keys (all optional — only provided keys are applied):
       :metadata     - map; replaces job metadata
       :priority     - integer 1-4
       :queue        - string queue name
       :scheduled-at - java.time.Instant; also sets state to :scheduled if in future, :available if in past
       :state        - keyword; only :available :scheduled :cancelled :discarded allowed
       :max-attempts - integer
       :tags         - vector of strings")
  (delete-job! [client tx job-id]
    "Hard-deletes a single job by ID regardless of state.
     Returns the deleted Job record, or nil if not found.")
  (get-job! [client tx job-id]
    "Fetches single job by ID. Returns Job record or nil.")
  (list-jobs! [client tx opts]
    "Lists jobs with optional filters. Returns vector of Job records.
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
       :after            - job ID cursor; returns jobs with id < after (for DESC pagination)")
  (expire-ttl-jobs! [client tx]
    "Discards all non-running jobs whose created_at + ttl_ms <= now.
     Targets :available, :scheduled, and :retryable states only.
     Returns count of expired jobs."))

(defprotocol FastBulkInsert
  (insert-many-fast! [client job-specs]
    "High-throughput batch insert. job-specs is a sequence of [kind args opts] tuples.
     Returns number of rows inserted (long).
     Limitations: no :unique-opts, no :tags, no :ephemeral, no returned Job records.
     PostgreSQL uses COPY FROM STDIN; MariaDB and SQLite use multi-row INSERT."))

(defprotocol Maintenance
  (reindex! [client]
    "Rebuilds database indexes to recover bloat. Only meaningful on PostgreSQL
     (uses REINDEX CONCURRENTLY). No-op on MariaDB and SQLite.
     Returns a map of {:index-name keyword, :status :reindexed|:skipped|:not-found}.
     Skipped when leftover _ccnew/_ccold artifacts from a previous failed
     concurrent reindex are detected — safe to retry later."))

(defprotocol Queues
  (upsert-queue! [client tx queue-name metadata]
    "Creates or updates a queue.")
  (pause-queue! [client tx queue-name]
    "Pauses a queue.")
  (resume-queue! [client tx queue-name]
    "Resumes a paused queue.")
  (queue-paused? [client tx queue-name]
    "Returns true if queue exists and is paused.")
  (list-queues! [client tx]
    "Returns all queues as a vector of maps."))
