# Table of contents
-  [`s-exp.drip`](#s-exp.drip)  - Drip: a Clojure job queue for MariaDB, PostgreSQL, and SQLite.
    -  [`cancel-job`](#s-exp.drip/cancel-job) - Cancels a job that is not already finalized.
    -  [`cancel-job!`](#s-exp.drip/cancel-job!) - Cancels a job that is not already finalized.
    -  [`complete-job`](#s-exp.drip/complete-job) - Marks a job as :completed.
    -  [`complete-job!`](#s-exp.drip/complete-job!) - Marks a job as :completed.
    -  [`default-retention-ms`](#s-exp.drip/default-retention-ms)
    -  [`default-retry-policy`](#s-exp.drip/default-retry-policy) - Default retry policy fn.
    -  [`delete-job`](#s-exp.drip/delete-job) - Hard-deletes a job by ID regardless of state.
    -  [`delete-job!`](#s-exp.drip/delete-job!) - Hard-deletes a job by ID regardless of state.
    -  [`delete-jobs`](#s-exp.drip/delete-jobs) - Deletes jobs matching opts.
    -  [`delete-jobs!`](#s-exp.drip/delete-jobs!) - Deletes jobs matching opts.
    -  [`discard-job`](#s-exp.drip/discard-job) - Moves a job to :discarded state.
    -  [`discard-job!`](#s-exp.drip/discard-job!) - Moves a job to :discarded state.
    -  [`fetch-jobs`](#s-exp.drip/fetch-jobs) - Atomically claims up to limit available jobs from queue.
    -  [`fetch-jobs!`](#s-exp.drip/fetch-jobs!) - Atomically claims up to limit available jobs from queue using the supplied tx.
    -  [`get-job`](#s-exp.drip/get-job) - Returns a Job record by ID using the client's own datasource, or nil if not found.
    -  [`get-job!`](#s-exp.drip/get-job!) - Returns a Job record by ID using the supplied tx, or nil if not found.
    -  [`insert-job`](#s-exp.drip/insert-job) - Inserts a job using the client's own datasource.
    -  [`insert-job!`](#s-exp.drip/insert-job!) - Inserts a job using the supplied tx.
    -  [`insert-many`](#s-exp.drip/insert-many) - Inserts multiple jobs in a single transaction using the client's own datasource.
    -  [`insert-many!`](#s-exp.drip/insert-many!) - Inserts multiple jobs in a single transaction using the supplied tx.
    -  [`insert-many-fast!`](#s-exp.drip/insert-many-fast!) - PostgreSQL only.
    -  [`list-jobs`](#s-exp.drip/list-jobs) - Lists jobs with optional filters.
    -  [`list-jobs!`](#s-exp.drip/list-jobs!) - Lists jobs with optional filters.
    -  [`list-queues`](#s-exp.drip/list-queues) - Returns all queues.
    -  [`list-queues!`](#s-exp.drip/list-queues!) - Returns all queues.
    -  [`migrate!`](#s-exp.drip/migrate!) - Creates all drip_* tables and indexes.
    -  [`pause-queue`](#s-exp.drip/pause-queue) - Pauses a queue - workers stop fetching from it.
    -  [`pause-queue!`](#s-exp.drip/pause-queue!) - Pauses a queue - workers stop fetching from it.
    -  [`record-output`](#s-exp.drip/record-output) - Merges {:output output} into the job's metadata column.
    -  [`record-output!`](#s-exp.drip/record-output!) - Merges {:output output} into the job's metadata column.
    -  [`rescue-stuck-jobs`](#s-exp.drip/rescue-stuck-jobs) - Rescues jobs stuck in :running state with attempted_at <= stuck-after.
    -  [`rescue-stuck-jobs!`](#s-exp.drip/rescue-stuck-jobs!) - Rescues jobs stuck in :running state with attempted_at <= stuck-after.
    -  [`resume-queue`](#s-exp.drip/resume-queue) - Resumes a paused queue.
    -  [`resume-queue!`](#s-exp.drip/resume-queue!) - Resumes a paused queue.
    -  [`retry-job`](#s-exp.drip/retry-job) - Forces a failed/cancelled/discarded job back to :available.
    -  [`retry-job!`](#s-exp.drip/retry-job!) - Forces a failed/cancelled/discarded job back to :available.
    -  [`snooze-job`](#s-exp.drip/snooze-job) - Reschedules a :running job to run again after duration without consuming a retry attempt.
    -  [`snooze-job!`](#s-exp.drip/snooze-job!) - Reschedules a :running job to run again after duration without consuming a retry attempt.
    -  [`start-executor!`](#s-exp.drip/start-executor!) - Starts polling queues and processing jobs.
    -  [`start-periodic-executor!`](#s-exp.drip/start-periodic-executor!) - Schedules periodic job insertions.
    -  [`stop-and-cancel!`](#s-exp.drip/stop-and-cancel!) - Immediately interrupts all in-flight jobs and shuts down the executor.
    -  [`stop-executor!`](#s-exp.drip/stop-executor!) - Gracefully stops the executor.
    -  [`stop-periodic-executor!`](#s-exp.drip/stop-periodic-executor!) - Stops the periodic job scheduler.
    -  [`update-job`](#s-exp.drip/update-job) - Updates writable fields of a job.
    -  [`update-job!`](#s-exp.drip/update-job!) - Updates writable fields of a job.
    -  [`upsert-queue`](#s-exp.drip/upsert-queue) - Creates or updates a queue.
    -  [`upsert-queue!`](#s-exp.drip/upsert-queue!) - Creates or updates a queue.
    -  [`with-tx`](#s-exp.drip/with-tx) - Opens a transaction from client and binds it to tx-sym, passing remaining args to next.jdbc/with-transaction.
-  [`s-exp.drip.client`](#s-exp.drip.client) 
    -  [`Jobs`](#s-exp.drip.client/jobs)
    -  [`Migration`](#s-exp.drip.client/migration)
    -  [`Notifications`](#s-exp.drip.client/notifications)
    -  [`Queues`](#s-exp.drip.client/queues)
    -  [`cancel-job!`](#s-exp.drip.client/cancel-job!) - Cancels a non-finalized job.
    -  [`complete-job!`](#s-exp.drip.client/complete-job!) - Marks job as :completed.
    -  [`delete-job!`](#s-exp.drip.client/delete-job!) - Hard-deletes a single job by ID regardless of state.
    -  [`delete-jobs!`](#s-exp.drip.client/delete-jobs!) - Deletes jobs matching opts.
    -  [`discard-job!`](#s-exp.drip.client/discard-job!) - Moves job to :discarded.
    -  [`fail-job!`](#s-exp.drip.client/fail-job!) - Records error, transitions to :retryable or :discarded.
    -  [`fetch-jobs!`](#s-exp.drip.client/fetch-jobs!) - Atomically claims up to limit available jobs using the supplied tx.
    -  [`get-job!`](#s-exp.drip.client/get-job!) - Fetches single job by ID.
    -  [`insert-job!`](#s-exp.drip.client/insert-job!) - Inserts a job using tx.
    -  [`list-jobs!`](#s-exp.drip.client/list-jobs!) - Lists jobs with optional filters.
    -  [`list-queues!`](#s-exp.drip.client/list-queues!) - Returns all queues as a vector of maps.
    -  [`migration-applied-sql`](#s-exp.drip.client/migration-applied-sql) - Returns SQL string to SELECT applied migration versions.
    -  [`migration-files`](#s-exp.drip.client/migration-files) - Returns ordered vector of [version resource-path] pairs.
    -  [`migration-record-sql`](#s-exp.drip.client/migration-record-sql) - Returns parameterized SQL string (with one ? for version) to INSERT a migration record.
    -  [`migration-table-ddl`](#s-exp.drip.client/migration-table-ddl) - Returns DDL string that creates the drip_migration tracking table.
    -  [`notify-job-available!`](#s-exp.drip.client/notify-job-available!) - Notifies listeners that jobs are available in queue.
    -  [`pause-queue!`](#s-exp.drip.client/pause-queue!) - Pauses a queue.
    -  [`promote-scheduled-jobs!`](#s-exp.drip.client/promote-scheduled-jobs!) - Moves :scheduled/:retryable jobs whose scheduled_at <= now to :available.
    -  [`queue-paused?`](#s-exp.drip.client/queue-paused?) - Returns true if queue exists and is paused.
    -  [`record-output!`](#s-exp.drip.client/record-output!) - Merges {:output output} into the job's metadata column.
    -  [`rescue-stuck-jobs!`](#s-exp.drip.client/rescue-stuck-jobs!) - Moves :running jobs with attempted_at <= stuck-after to :retryable or :discarded.
    -  [`resume-queue!`](#s-exp.drip.client/resume-queue!) - Resumes a paused queue.
    -  [`retry-job!`](#s-exp.drip.client/retry-job!) - Forces job back to :available.
    -  [`snooze-job!`](#s-exp.drip.client/snooze-job!) - Reschedules a :running job to run again after duration (ms number or duration string e.g.
    -  [`start-listener!`](#s-exp.drip.client/start-listener!) - Starts background LISTEN connection, calls (on-notify queue) on each notification.
    -  [`stop-listener!`](#s-exp.drip.client/stop-listener!) - Stops listener returned by start-listener!.
    -  [`update-job!`](#s-exp.drip.client/update-job!) - Updates writable fields of a job.
    -  [`upsert-queue!`](#s-exp.drip.client/upsert-queue!) - Creates or updates a queue.
-  [`s-exp.drip.client.mariadb`](#s-exp.drip.client.mariadb) 
    -  [`make-client`](#s-exp.drip.client.mariadb/make-client) - Returns a MariaDB client.
-  [`s-exp.drip.client.postgres`](#s-exp.drip.client.postgres)  - PostgreSQL client.
    -  [`insert-many-fast!`](#s-exp.drip.client.postgres/insert-many-fast!) - High-throughput batch insert using PostgreSQL COPY FROM STDIN.
    -  [`make-client`](#s-exp.drip.client.postgres/make-client) - Returns a PostgreSQL client.
-  [`s-exp.drip.client.sqlite`](#s-exp.drip.client.sqlite)  - SQLite client.
    -  [`make-client`](#s-exp.drip.client.sqlite/make-client) - Returns a SQLite client.
-  [`s-exp.drip.db`](#s-exp.drip.db) 
    -  [`->json`](#s-exp.drip.db/->json)
    -  [`->json-str`](#s-exp.drip.db/->json-str)
    -  [`<-json`](#s-exp.drip.db/<-json)
    -  [`<-json-metadata`](#s-exp.drip.db/<-json-metadata)
    -  [`compute-unique-key`](#s-exp.drip.db/compute-unique-key) - Computes a 32-byte SHA-256 unique key for a job.
    -  [`instant->str`](#s-exp.drip.db/instant->str)
    -  [`instant->ts`](#s-exp.drip.db/instant->ts)
    -  [`jdbc-opts`](#s-exp.drip.db/jdbc-opts)
    -  [`mapper`](#s-exp.drip.db/mapper)
    -  [`migrate!`](#s-exp.drip.db/migrate!) - Runs pending migrations against the datasource.
    -  [`ts->instant`](#s-exp.drip.db/ts->instant)
    -  [`with-tx`](#s-exp.drip.db/with-tx) - Opens a transaction from client and binds it to tx-sym, passing remaining args to next.jdbc/with-transaction.
-  [`s-exp.drip.job`](#s-exp.drip.job) 
    -  [`bitmask->states`](#s-exp.drip.job/bitmask->states) - Converts an integer bitmask to a set of state keywords.
    -  [`default-insert-opts`](#s-exp.drip.job/default-insert-opts)
    -  [`default-retry-policy`](#s-exp.drip.job/default-retry-policy) - Returns a java.time.Instant for the next retry.
    -  [`default-unique-states`](#s-exp.drip.job/default-unique-states)
    -  [`state->bit`](#s-exp.drip.job/state->bit)
    -  [`state-in-bitmask?`](#s-exp.drip.job/state-in-bitmask?) - Returns true if the given state keyword is represented in the bitmask.
    -  [`states`](#s-exp.drip.job/states)
    -  [`states->bitmask`](#s-exp.drip.job/states->bitmask) - Converts a collection of state keywords to an integer bitmask.
-  [`s-exp.drip.periodic`](#s-exp.drip.periodic) 
    -  [`start-periodic-executor!`](#s-exp.drip.periodic/start-periodic-executor!) - Schedules periodic job insertions for a sequence of spec maps.
    -  [`stop-periodic-executor!`](#s-exp.drip.periodic/stop-periodic-executor!) - Shuts down the periodic executor.
-  [`s-exp.drip.worker`](#s-exp.drip.worker) 
    -  [`default-retention-ms`](#s-exp.drip.worker/default-retention-ms)
    -  [`start-executor!`](#s-exp.drip.worker/start-executor!) - Starts a job executor that polls queues and dispatches jobs to workers.
    -  [`stop-and-cancel!`](#s-exp.drip.worker/stop-and-cancel!) - Immediately cancels all in-flight jobs by interrupting their threads, then shuts down.
    -  [`stop-executor!`](#s-exp.drip.worker/stop-executor!) - Gracefully shuts down the executor.

-----
# <a name="s-exp.drip">s-exp.drip</a>


Drip: a Clojure job queue for MariaDB, PostgreSQL, and SQLite.

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
          :registry {"send_email" (fn [_ job] (send-email! (:args job)))}
          :queues ["default" "priority"]}))

     ;; 4. Insert jobs
     (drip/insert-job client "send_email" {:to "user@example.com"})

     ;; 5. Stop on shutdown
     (drip/stop-executor! executor)




## <a name="s-exp.drip/cancel-job">`cancel-job`</a>
``` clojure

(cancel-job client job-id)
```
Function.

Cancels a job that is not already finalized. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L219-L223">Source</a></sub></p>

## <a name="s-exp.drip/cancel-job!">`cancel-job!`</a>
``` clojure

(cancel-job! client tx job-id)
```
Function.

Cancels a job that is not already finalized. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L225-L228">Source</a></sub></p>

## <a name="s-exp.drip/complete-job">`complete-job`</a>
``` clojure

(complete-job client job-id)
```
Function.

Marks a job as :completed. Uses the client's own datasource. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L208-L212">Source</a></sub></p>

## <a name="s-exp.drip/complete-job!">`complete-job!`</a>
``` clojure

(complete-job! client tx job-id)
```
Function.

Marks a job as :completed. Uses the supplied tx. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L214-L217">Source</a></sub></p>

## <a name="s-exp.drip/default-retention-ms">`default-retention-ms`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L414-L414">Source</a></sub></p>

## <a name="s-exp.drip/default-retry-policy">`default-retry-policy`</a>




Default retry policy fn. Takes attempt (1-based long), returns java.time.Instant.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L410-L412">Source</a></sub></p>

## <a name="s-exp.drip/delete-job">`delete-job`</a>
``` clojure

(delete-job client job-id)
```
Function.

Hard-deletes a job by ID regardless of state. Uses the client's own datasource.
   Returns the deleted Job record, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L253-L258">Source</a></sub></p>

## <a name="s-exp.drip/delete-job!">`delete-job!`</a>
``` clojure

(delete-job! client tx job-id)
```
Function.

Hard-deletes a job by ID regardless of state. Uses the supplied tx.
   Returns the deleted Job record, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L260-L264">Source</a></sub></p>

## <a name="s-exp.drip/delete-jobs">`delete-jobs`</a>
``` clojure

(delete-jobs client opts)
```
Function.

Deletes jobs matching opts. Uses the client's own datasource.
   Returns count of deleted jobs.
   opts keys:
     :states           - set/seq of state keywords (required)
     :kinds            - coll of kind strings
     :queues           - coll of queue names
     :priorities       - coll of priority ints
     :finalized-before - java.time.Instant; only delete with finalized_at <= this
     :created-before   - java.time.Instant; only delete with created_at <= this
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L295-L307">Source</a></sub></p>

## <a name="s-exp.drip/delete-jobs!">`delete-jobs!`</a>
``` clojure

(delete-jobs! client tx opts)
```
Function.

Deletes jobs matching opts. Uses the supplied tx.
   Returns count of deleted jobs. See delete-jobs for opts documentation.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L309-L313">Source</a></sub></p>

## <a name="s-exp.drip/discard-job">`discard-job`</a>
``` clojure

(discard-job client job-id)
```
Function.

Moves a job to :discarded state. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L242-L246">Source</a></sub></p>

## <a name="s-exp.drip/discard-job!">`discard-job!`</a>
``` clojure

(discard-job! client tx job-id)
```
Function.

Moves a job to :discarded state. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L248-L251">Source</a></sub></p>

## <a name="s-exp.drip/fetch-jobs">`fetch-jobs`</a>
``` clojure

(fetch-jobs client queue worker-id & {:as opts})
```
Function.

Atomically claims up to limit available jobs from queue.
   Opens its own transaction. Returns vector of Job records.
   opts: :limit (default 10)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L122-L128">Source</a></sub></p>

## <a name="s-exp.drip/fetch-jobs!">`fetch-jobs!`</a>
``` clojure

(fetch-jobs! client tx queue worker-id opts)
```
Function.

Atomically claims up to limit available jobs from queue using the supplied tx.
   Returns vector of Job records.
   opts: :limit (default 10)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L130-L135">Source</a></sub></p>

## <a name="s-exp.drip/get-job">`get-job`</a>
``` clojure

(get-job client job-id)
```
Function.

Returns a Job record by ID using the client's own datasource, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L137-L141">Source</a></sub></p>

## <a name="s-exp.drip/get-job!">`get-job!`</a>
``` clojure

(get-job! client tx job-id)
```
Function.

Returns a Job record by ID using the supplied tx, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L143-L146">Source</a></sub></p>

## <a name="s-exp.drip/insert-job">`insert-job`</a>
``` clojure

(insert-job client kind args & {:as opts})
```
Function.

Inserts a job using the client's own datasource. Returns the created Job record.
   `kind` - string job type identifier
   `args` - map of job arguments (JSON-serializable)
   `opts` - insert opts as keyword args or a map, e.g.:
     (insert-job client "k" {} :queue "bulk" :priority 2)
     (insert-job client "k" {} {:queue "bulk" :priority 2})
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L63-L74">Source</a></sub></p>

## <a name="s-exp.drip/insert-job!">`insert-job!`</a>
``` clojure

(insert-job! client tx kind args & {:as opts})
```
Function.

Inserts a job using the supplied tx. Returns the created Job record.
   `kind` - string job type identifier
   `args` - map of job arguments (JSON-serializable)
   `opts` - insert opts as keyword args or a map, e.g.:
     (insert-job! client tx "k" {} :queue "bulk" :priority 2)
     (insert-job! client tx "k" {} {:queue "bulk" :priority 2})
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L76-L84">Source</a></sub></p>

## <a name="s-exp.drip/insert-many">`insert-many`</a>
``` clojure

(insert-many client job-specs)
```
Function.

Inserts multiple jobs in a single transaction using the client's own datasource.
   `job-specs` - sequence of [kind args opts] tuples.
   Returns a vector of Job records.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L86-L96">Source</a></sub></p>

## <a name="s-exp.drip/insert-many!">`insert-many!`</a>
``` clojure

(insert-many! client tx job-specs)
```
Function.

Inserts multiple jobs in a single transaction using the supplied tx.
   `job-specs` - sequence of [kind args opts] tuples.
   Returns a vector of Job records.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L98-L104">Source</a></sub></p>

## <a name="s-exp.drip/insert-many-fast!">`insert-many-fast!`</a>
``` clojure

(insert-many-fast! conn job-specs)
```
Function.

PostgreSQL only. High-throughput batch insert using COPY FROM STDIN.
   `conn` must be a raw java.sql.Connection — obtain one from the DataSource directly:
     (with-open [conn (.getConnection (:ds client))]
       (drip/insert-many-fast! conn job-specs))
   `job-specs` - sequence of [kind args opts] tuples.
   Returns the number of rows inserted (long).
   Limitations vs insert-many:
     - Does not support :unique-opts deduplication
     - Does not return Job records
     - PostgreSQL only (uses org.postgresql.copy.CopyManager)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L106-L120">Source</a></sub></p>

## <a name="s-exp.drip/list-jobs">`list-jobs`</a>
``` clojure

(list-jobs client opts)
```
Function.

Lists jobs with optional filters. Uses the client's own datasource.
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
     :after            - job ID cursor for pagination; returns jobs with id < after
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L148-L166">Source</a></sub></p>

## <a name="s-exp.drip/list-jobs!">`list-jobs!`</a>
``` clojure

(list-jobs! client tx opts)
```
Function.

Lists jobs with optional filters. Uses the supplied tx.
   See list-jobs for opts documentation.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L168-L172">Source</a></sub></p>

## <a name="s-exp.drip/list-queues">`list-queues`</a>
``` clojure

(list-queues client)
```
Function.

Returns all queues. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L352-L356">Source</a></sub></p>

## <a name="s-exp.drip/list-queues!">`list-queues!`</a>
``` clojure

(list-queues! client tx)
```
Function.

Returns all queues. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L358-L361">Source</a></sub></p>

## <a name="s-exp.drip/migrate!">`migrate!`</a>
``` clojure

(migrate! client)
```
Function.

Creates all drip_* tables and indexes. Idempotent - safe to call on startup.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L54-L57">Source</a></sub></p>

## <a name="s-exp.drip/pause-queue">`pause-queue`</a>
``` clojure

(pause-queue client queue-name)
```
Function.

Pauses a queue - workers stop fetching from it. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L330-L334">Source</a></sub></p>

## <a name="s-exp.drip/pause-queue!">`pause-queue!`</a>
``` clojure

(pause-queue! client tx queue-name)
```
Function.

Pauses a queue - workers stop fetching from it. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L336-L339">Source</a></sub></p>

## <a name="s-exp.drip/record-output">`record-output`</a>
``` clojure

(record-output client job-id output)
```
Function.

Merges {:output output} into the job's metadata column.
   Uses the client's own datasource. Returns updated Job record.
   Output is accessible as (get-in job [:metadata "output"]) after completion.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L194-L200">Source</a></sub></p>

## <a name="s-exp.drip/record-output!">`record-output!`</a>
``` clojure

(record-output! client tx job-id output)
```
Function.

Merges {:output output} into the job's metadata column. Uses the supplied tx.
   Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L202-L206">Source</a></sub></p>

## <a name="s-exp.drip/rescue-stuck-jobs">`rescue-stuck-jobs`</a>
``` clojure

(rescue-stuck-jobs client stuck-after retry-policy)
```
Function.

Rescues jobs stuck in :running state with attempted_at <= stuck-after.
   Transitions each to :retryable or :discarded (based on attempt vs max-attempts).
   Uses the client's own datasource. Returns count of rescued jobs.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L281-L287">Source</a></sub></p>

## <a name="s-exp.drip/rescue-stuck-jobs!">`rescue-stuck-jobs!`</a>
``` clojure

(rescue-stuck-jobs! client tx stuck-after retry-policy)
```
Function.

Rescues jobs stuck in :running state with attempted_at <= stuck-after.
   Uses the supplied tx. Returns count of rescued jobs.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L289-L293">Source</a></sub></p>

## <a name="s-exp.drip/resume-queue">`resume-queue`</a>
``` clojure

(resume-queue client queue-name)
```
Function.

Resumes a paused queue. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L341-L345">Source</a></sub></p>

## <a name="s-exp.drip/resume-queue!">`resume-queue!`</a>
``` clojure

(resume-queue! client tx queue-name)
```
Function.

Resumes a paused queue. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L347-L350">Source</a></sub></p>

## <a name="s-exp.drip/retry-job">`retry-job`</a>
``` clojure

(retry-job client job-id)
```
Function.

Forces a failed/cancelled/discarded job back to :available.
   Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L230-L235">Source</a></sub></p>

## <a name="s-exp.drip/retry-job!">`retry-job!`</a>
``` clojure

(retry-job! client tx job-id)
```
Function.

Forces a failed/cancelled/discarded job back to :available. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L237-L240">Source</a></sub></p>

## <a name="s-exp.drip/snooze-job">`snooze-job`</a>
``` clojure

(snooze-job client job-id duration)
```
Function.

Reschedules a :running job to run again after duration without consuming a retry attempt.
   duration - ms number or duration string (e.g. "1h", "30m", "90s").
   Uses the client's own datasource. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L266-L272">Source</a></sub></p>

## <a name="s-exp.drip/snooze-job!">`snooze-job!`</a>
``` clojure

(snooze-job! client tx job-id duration)
```
Function.

Reschedules a :running job to run again after duration without consuming a retry attempt.
   duration - ms number or duration string (e.g. "1h", "30m", "90s").
   Uses the supplied tx. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L274-L279">Source</a></sub></p>

## <a name="s-exp.drip/start-executor!">`start-executor!`</a>
``` clojure

(start-executor! opts)
```
Function.

Starts polling queues and processing jobs.
   :client and :registry are required. See s-exp.drip.worker/start-executor! for full option docs.

   Registry handlers receive [client job] as two arguments.
   Handlers must explicitly call complete-job!, snooze-job!, etc. to manage job state.
   Throwing any Throwable signals failure and triggers retry or discard.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L367-L375">Source</a></sub></p>

## <a name="s-exp.drip/start-periodic-executor!">`start-periodic-executor!`</a>
``` clojure

(start-periodic-executor! client specs)
```
Function.

Schedules periodic job insertions. `specs` is a sequence of PeriodicSpec maps.
   Returns a scheduler; stop with stop-periodic-executor!.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L395-L399">Source</a></sub></p>

## <a name="s-exp.drip/stop-and-cancel!">`stop-and-cancel!`</a>
``` clojure

(stop-and-cancel! executor)
```
Function.

Immediately interrupts all in-flight jobs and shuts down the executor.
   In-flight jobs remain in :running state; rescue-stuck-jobs will requeue them.
   Use stop-executor! instead when you want to wait for jobs to finish gracefully.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L384-L389">Source</a></sub></p>

## <a name="s-exp.drip/stop-executor!">`stop-executor!`</a>
``` clojure

(stop-executor! executor)
(stop-executor! executor timeout-ms)
```
Function.

Gracefully stops the executor. Optional second arg: timeout-ms (default 30000).
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L377-L382">Source</a></sub></p>

## <a name="s-exp.drip/stop-periodic-executor!">`stop-periodic-executor!`</a>
``` clojure

(stop-periodic-executor! scheduler)
```
Function.

Stops the periodic job scheduler.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L401-L404">Source</a></sub></p>

## <a name="s-exp.drip/update-job">`update-job`</a>
``` clojure

(update-job client job-id opts)
```
Function.

Updates writable fields of a job. Uses the client's own datasource. Returns updated Job record.
   opts keys (all optional — only provided keys are applied):
     :metadata     - map; replaces job metadata
     :priority     - integer 1-4
     :queue        - string queue name
     :scheduled-at - java.time.Instant; also transitions state to :scheduled (future) or :available (past)
     :state        - keyword; only :available :scheduled :cancelled :discarded allowed
     :max-attempts - integer
     :tags         - vector of strings
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L174-L186">Source</a></sub></p>

## <a name="s-exp.drip/update-job!">`update-job!`</a>
``` clojure

(update-job! client tx job-id opts)
```
Function.

Updates writable fields of a job. Uses the supplied tx. Returns updated Job record.
   See update-job for opts documentation.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L188-L192">Source</a></sub></p>

## <a name="s-exp.drip/upsert-queue">`upsert-queue`</a>
``` clojure

(upsert-queue client queue-name metadata)
```
Function.

Creates or updates a queue. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L319-L323">Source</a></sub></p>

## <a name="s-exp.drip/upsert-queue!">`upsert-queue!`</a>
``` clojure

(upsert-queue! client tx queue-name metadata)
```
Function.

Creates or updates a queue. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L325-L328">Source</a></sub></p>

## <a name="s-exp.drip/with-tx">`with-tx`</a>
``` clojure

(with-tx [tx-sym client & jdbc-opts] & body)
```
Macro.

Opens a transaction from client and binds it to tx-sym, passing remaining
   args to next.jdbc/with-transaction. Body executes within the transaction.

   Example:
     (drip/with-tx [tx client]
       (drip/insert-job! client tx "k" {} nil)
       (my-business-write! tx data))
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L39-L48">Source</a></sub></p>

-----
# <a name="s-exp.drip.client">s-exp.drip.client</a>






## <a name="s-exp.drip.client/jobs">`Jobs`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L33-L100">Source</a></sub></p>

## <a name="s-exp.drip.client/migration">`Migration`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L12-L21">Source</a></sub></p>

## <a name="s-exp.drip.client/notifications">`Notifications`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L23-L31">Source</a></sub></p>

## <a name="s-exp.drip.client/queues">`Queues`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L102-L112">Source</a></sub></p>

## <a name="s-exp.drip.client/cancel-job!">`cancel-job!`</a>
``` clojure

(cancel-job! client tx job-id)
```
Function.

Cancels a non-finalized job. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L46-L47">Source</a></sub></p>

## <a name="s-exp.drip.client/complete-job!">`complete-job!`</a>
``` clojure

(complete-job! client tx job-id)
```
Function.

Marks job as :completed. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L42-L43">Source</a></sub></p>

## <a name="s-exp.drip.client/delete-job!">`delete-job!`</a>
``` clojure

(delete-job! client tx job-id)
```
Function.

Hard-deletes a single job by ID regardless of state.
     Returns the deleted Job record, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L80-L82">Source</a></sub></p>

## <a name="s-exp.drip.client/delete-jobs!">`delete-jobs!`</a>
``` clojure

(delete-jobs! client tx opts)
```
Function.

Deletes jobs matching opts. Returns count of deleted jobs.
     opts keys:
       :states           - set/seq of state keywords (required)
       :kinds            - coll of kind strings
       :queues           - coll of queue names
       :priorities       - coll of priority ints
       :finalized-before - java.time.Instant; only delete with finalized_at <= this
       :created-before   - java.time.Instant; only delete with created_at <= this
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L61-L69">Source</a></sub></p>

## <a name="s-exp.drip.client/discard-job!">`discard-job!`</a>
``` clojure

(discard-job! client tx job-id)
```
Function.

Moves job to :discarded. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L50-L51">Source</a></sub></p>

## <a name="s-exp.drip.client/fail-job!">`fail-job!`</a>
``` clojure

(fail-job! client tx job-id error-map retry-policy)
```
Function.

Records error, transitions to :retryable or :discarded. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L44-L45">Source</a></sub></p>

## <a name="s-exp.drip.client/fetch-jobs!">`fetch-jobs!`</a>
``` clojure

(fetch-jobs! client tx queue worker-id opts)
```
Function.

Atomically claims up to limit available jobs using the supplied tx.
     opts: :limit (default 10). Returns vector of Job records.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L36-L38">Source</a></sub></p>

## <a name="s-exp.drip.client/get-job!">`get-job!`</a>
``` clojure

(get-job! client tx job-id)
```
Function.

Fetches single job by ID. Returns Job record or nil.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L83-L84">Source</a></sub></p>

## <a name="s-exp.drip.client/insert-job!">`insert-job!`</a>
``` clojure

(insert-job! client tx kind args opts)
```
Function.

Inserts a job using tx. Returns a Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L34-L35">Source</a></sub></p>

## <a name="s-exp.drip.client/list-jobs!">`list-jobs!`</a>
``` clojure

(list-jobs! client tx opts)
```
Function.

Lists jobs with optional filters. Returns vector of Job records.
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
       :after            - job ID cursor; returns jobs with id < after (for DESC pagination)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L85-L100">Source</a></sub></p>

## <a name="s-exp.drip.client/list-queues!">`list-queues!`</a>
``` clojure

(list-queues! client tx)
```
Function.

Returns all queues as a vector of maps.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L111-L112">Source</a></sub></p>

## <a name="s-exp.drip.client/migration-applied-sql">`migration-applied-sql`</a>
``` clojure

(migration-applied-sql client)
```
Function.

Returns SQL string to SELECT applied migration versions.
     Must return rows with a :version column.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L17-L19">Source</a></sub></p>

## <a name="s-exp.drip.client/migration-files">`migration-files`</a>
``` clojure

(migration-files client)
```
Function.

Returns ordered vector of [version resource-path] pairs.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L15-L16">Source</a></sub></p>

## <a name="s-exp.drip.client/migration-record-sql">`migration-record-sql`</a>
``` clojure

(migration-record-sql client)
```
Function.

Returns parameterized SQL string (with one ? for version) to INSERT a migration record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L20-L21">Source</a></sub></p>

## <a name="s-exp.drip.client/migration-table-ddl">`migration-table-ddl`</a>
``` clojure

(migration-table-ddl client)
```
Function.

Returns DDL string that creates the drip_migration tracking table.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L13-L14">Source</a></sub></p>

## <a name="s-exp.drip.client/notify-job-available!">`notify-job-available!`</a>
``` clojure

(notify-job-available! client queue)
```
Function.

Notifies listeners that jobs are available in queue.
     No-op on MariaDB/SQLite; calls pg_notify on PostgreSQL.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L24-L26">Source</a></sub></p>

## <a name="s-exp.drip.client/pause-queue!">`pause-queue!`</a>
``` clojure

(pause-queue! client tx queue-name)
```
Function.

Pauses a queue.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L105-L106">Source</a></sub></p>

## <a name="s-exp.drip.client/promote-scheduled-jobs!">`promote-scheduled-jobs!`</a>
``` clojure

(promote-scheduled-jobs! client tx)
```
Function.

Moves :scheduled/:retryable jobs whose scheduled_at <= now to :available.
     Returns count of promoted jobs.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L55-L57">Source</a></sub></p>

## <a name="s-exp.drip.client/queue-paused?">`queue-paused?`</a>
``` clojure

(queue-paused? client tx queue-name)
```
Function.

Returns true if queue exists and is paused.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L109-L110">Source</a></sub></p>

## <a name="s-exp.drip.client/record-output!">`record-output!`</a>
``` clojure

(record-output! client tx job-id output)
```
Function.

Merges {:output output} into the job's metadata column. Returns updated Job record.
     Output is accessible as (get-in job [:metadata "output"]) after completion.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L39-L41">Source</a></sub></p>

## <a name="s-exp.drip.client/rescue-stuck-jobs!">`rescue-stuck-jobs!`</a>
``` clojure

(rescue-stuck-jobs! client tx stuck-after retry-policy)
```
Function.

Moves :running jobs with attempted_at <= stuck-after to :retryable or :discarded.
     Appends a rescue error entry to each job. Returns count of rescued jobs.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L58-L60">Source</a></sub></p>

## <a name="s-exp.drip.client/resume-queue!">`resume-queue!`</a>
``` clojure

(resume-queue! client tx queue-name)
```
Function.

Resumes a paused queue.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L107-L108">Source</a></sub></p>

## <a name="s-exp.drip.client/retry-job!">`retry-job!`</a>
``` clojure

(retry-job! client tx job-id)
```
Function.

Forces job back to :available. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L48-L49">Source</a></sub></p>

## <a name="s-exp.drip.client/snooze-job!">`snooze-job!`</a>
``` clojure

(snooze-job! client tx job-id duration)
```
Function.

Reschedules a :running job to run again after duration (ms number or duration string e.g. "1h")
     without consuming a retry attempt. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L52-L54">Source</a></sub></p>

## <a name="s-exp.drip.client/start-listener!">`start-listener!`</a>
``` clojure

(start-listener! client on-notify)
```
Function.

Starts background LISTEN connection, calls (on-notify queue) on each
     notification. Returns opaque listener handle (nil if unsupported).
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L27-L29">Source</a></sub></p>

## <a name="s-exp.drip.client/stop-listener!">`stop-listener!`</a>
``` clojure

(stop-listener! client listener)
```
Function.

Stops listener returned by start-listener!. No-op if listener is nil.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L30-L31">Source</a></sub></p>

## <a name="s-exp.drip.client/update-job!">`update-job!`</a>
``` clojure

(update-job! client tx job-id opts)
```
Function.

Updates writable fields of a job. Returns updated Job record, or nil if not found.
     opts keys (all optional — only provided keys are applied):
       :metadata     - map; replaces job metadata
       :priority     - integer 1-4
       :queue        - string queue name
       :scheduled-at - java.time.Instant; also sets state to :scheduled if in future, :available if in past
       :state        - keyword; only :available :scheduled :cancelled :discarded allowed
       :max-attempts - integer
       :tags         - vector of strings
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L70-L79">Source</a></sub></p>

## <a name="s-exp.drip.client/upsert-queue!">`upsert-queue!`</a>
``` clojure

(upsert-queue! client tx queue-name metadata)
```
Function.

Creates or updates a queue.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L103-L104">Source</a></sub></p>

-----
# <a name="s-exp.drip.client.mariadb">s-exp.drip.client.mariadb</a>






## <a name="s-exp.drip.client.mariadb/make-client">`make-client`</a>
``` clojure

(make-client ds)
```
Function.

Returns a MariaDB client. `ds` is a javax.sql.DataSource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client/mariadb.clj#L418-L421">Source</a></sub></p>

-----
# <a name="s-exp.drip.client.postgres">s-exp.drip.client.postgres</a>


PostgreSQL client.




## <a name="s-exp.drip.client.postgres/insert-many-fast!">`insert-many-fast!`</a>
``` clojure

(insert-many-fast! conn job-specs)
```
Function.

High-throughput batch insert using PostgreSQL COPY FROM STDIN.
   `conn` must be a java.sql.Connection (unwrapped, not inside next.jdbc tx).
   `job-specs` - sequence of [kind args opts] tuples.
   Returns the number of rows inserted.
   Does NOT support :unique-opts deduplication.
   Does NOT return Job records — use insert-many for that.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client/postgres.clj#L553-L569">Source</a></sub></p>

## <a name="s-exp.drip.client.postgres/make-client">`make-client`</a>
``` clojure

(make-client ds)
```
Function.

Returns a PostgreSQL client. `ds` is a javax.sql.DataSource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client/postgres.clj#L571-L574">Source</a></sub></p>

-----
# <a name="s-exp.drip.client.sqlite">s-exp.drip.client.sqlite</a>


SQLite client.
   Key differences from MariaDB/PostgreSQL:
     - No FOR UPDATE SKIP LOCKED; SQLite serializes writes at the DB level
     - Timestamps stored as ISO-8601 TEXT
     - JSON stored as TEXT; json_insert/json_array require SQLite 3.38.0+
     - INTEGER PRIMARY KEY AUTOINCREMENT; generated key is :last_insert_rowid()
     - No NOTIFY/LISTEN




## <a name="s-exp.drip.client.sqlite/make-client">`make-client`</a>
``` clojure

(make-client ds)
```
Function.

Returns a SQLite client. `ds` is a javax.sql.DataSource.
   Enables WAL mode and sets a 5-second busy timeout so concurrent
   worker threads can share the same database file without SQLITE_BUSY errors.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client/sqlite.clj#L422-L430">Source</a></sub></p>

-----
# <a name="s-exp.drip.db">s-exp.drip.db</a>






## <a name="s-exp.drip.db/->json">`->json`</a>
``` clojure

(->json v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L40-L41">Source</a></sub></p>

## <a name="s-exp.drip.db/->json-str">`->json-str`</a>
``` clojure

(->json-str v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L43-L44">Source</a></sub></p>

## <a name="s-exp.drip.db/<-json">`<-json`</a>
``` clojure

(<-json v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L57-L58">Source</a></sub></p>

## <a name="s-exp.drip.db/<-json-metadata">`<-json-metadata`</a>
``` clojure

(<-json-metadata v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L60-L62">Source</a></sub></p>

## <a name="s-exp.drip.db/compute-unique-key">`compute-unique-key`</a>
``` clojure

(compute-unique-key kind encoded-args queue now unique-opts)
```
Function.

Computes a 32-byte SHA-256 unique key for a job.
   Returns nil when unique-opts is nil (no uniqueness constraint).
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L122-L141">Source</a></sub></p>

## <a name="s-exp.drip.db/instant->str">`instant->str`</a>
``` clojure

(instant->str i)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L89-L90">Source</a></sub></p>

## <a name="s-exp.drip.db/instant->ts">`instant->ts`</a>
``` clojure

(instant->ts i)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L86-L87">Source</a></sub></p>

## <a name="s-exp.drip.db/jdbc-opts">`jdbc-opts`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L96-L97">Source</a></sub></p>

## <a name="s-exp.drip.db/mapper">`mapper`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L20-L28">Source</a></sub></p>

## <a name="s-exp.drip.db/migrate!">`migrate!`</a>
``` clojure

(migrate! c)
```
Function.

Runs pending migrations against the datasource. Idempotent - safe to call
   on every application startup. Accepts a Client record (from make-client).
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L184-L197">Source</a></sub></p>

## <a name="s-exp.drip.db/ts->instant">`ts->instant`</a>
``` clojure

(ts->instant v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L76-L84">Source</a></sub></p>

## <a name="s-exp.drip.db/with-tx">`with-tx`</a>
``` clojure

(with-tx [tx-sym client & jdbc-opts] & body)
```
Macro.

Opens a transaction from client and binds it to tx-sym, passing remaining
   args to next.jdbc/with-transaction. Body executes within the transaction.

   Example:
     (db/with-tx [tx client]
       (insert-job! client tx "k" {} nil)
       (my-business-write! tx data))
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L99-L109">Source</a></sub></p>

-----
# <a name="s-exp.drip.job">s-exp.drip.job</a>






## <a name="s-exp.drip.job/bitmask->states">`bitmask->states`</a>
``` clojure

(bitmask->states mask)
```
Function.

Converts an integer bitmask to a set of state keywords.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L31-L39">Source</a></sub></p>

## <a name="s-exp.drip.job/default-insert-opts">`default-insert-opts`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L50-L55">Source</a></sub></p>

## <a name="s-exp.drip.job/default-retry-policy">`default-retry-policy`</a>
``` clojure

(default-retry-policy attempt)
```
Function.

Returns a java.time.Instant for the next retry. attempt is 1-based.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L68-L71">Source</a></sub></p>

## <a name="s-exp.drip.job/default-unique-states">`default-unique-states`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L47-L48">Source</a></sub></p>

## <a name="s-exp.drip.job/state->bit">`state->bit`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L11-L19">Source</a></sub></p>

## <a name="s-exp.drip.job/state-in-bitmask?">`state-in-bitmask?`</a>
``` clojure

(state-in-bitmask? state mask)
```
Function.

Returns true if the given state keyword is represented in the bitmask.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L41-L44">Source</a></sub></p>

## <a name="s-exp.drip.job/states">`states`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L21-L21">Source</a></sub></p>

## <a name="s-exp.drip.job/states->bitmask">`states->bitmask`</a>
``` clojure

(states->bitmask state-coll)
```
Function.

Converts a collection of state keywords to an integer bitmask.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L23-L29">Source</a></sub></p>

-----
# <a name="s-exp.drip.periodic">s-exp.drip.periodic</a>






## <a name="s-exp.drip.periodic/start-periodic-executor!">`start-periodic-executor!`</a>
``` clojure

(start-periodic-executor! c specs)
```
Function.

Schedules periodic job insertions for a sequence of spec maps.
   Each spec fires at the start of every period interval.
   Duplicate insertions within a period are silently discarded (unique key conflict).

   Spec keys:
     :kind   - job kind string
     :args   - job args map
     :period - period as ms number or duration string (e.g. "1h", "30m")
     :queue  - queue name (default "default")
     :opts   - extra insert opts map or nil

   `client` is a Client record (from make-client).
   Returns a ScheduledExecutorService. Stop with stop-periodic-executor!.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/periodic.clj#L24-L58">Source</a></sub></p>

## <a name="s-exp.drip.periodic/stop-periodic-executor!">`stop-periodic-executor!`</a>
``` clojure

(stop-periodic-executor! scheduler)
```
Function.

Shuts down the periodic executor.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/periodic.clj#L60-L63">Source</a></sub></p>

-----
# <a name="s-exp.drip.worker">s-exp.drip.worker</a>






## <a name="s-exp.drip.worker/default-retention-ms">`default-retention-ms`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/worker.clj#L85-L88">Source</a></sub></p>

## <a name="s-exp.drip.worker/start-executor!">`start-executor!`</a>
``` clojure

(start-executor!
 {:keys
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
   retention],
  :or
  {queues ["default"],
   concurrency 10,
   poll-interval-ms 1000,
   retry-policy job/default-retry-policy,
   retry-policies {},
   job-timeouts {},
   rescue-after-ms 3600000,
   retention default-retention-ms}})
```
Function.

Starts a job executor that polls queues and dispatches jobs to workers.

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
     :queues            - vector of queue names to consume (default ["default"])
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

   Returns an Executor record. Stop with stop-executor!.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/worker.clj#L154-L239">Source</a></sub></p>

## <a name="s-exp.drip.worker/stop-and-cancel!">`stop-and-cancel!`</a>
``` clojure

(stop-and-cancel! {:keys [client task-executor scheduler listener running?]})
```
Function.

Immediately cancels all in-flight jobs by interrupting their threads, then shuts down.
   In-flight jobs remain in :running state and will be rescued by rescue-stuck-jobs on the
   next executor startup (or via the periodic rescue in another running executor).
   Returns the list of cancelled Futures from shutdownNow.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/worker.clj#L260-L272">Source</a></sub></p>

## <a name="s-exp.drip.worker/stop-executor!">`stop-executor!`</a>
``` clojure

(stop-executor! executor)
(stop-executor! {:keys [client task-executor scheduler listener running?]} timeout-ms)
```
Function.

Gracefully shuts down the executor.
   Waits up to timeout-ms for in-flight jobs to finish (default 30s).
   Returns true if clean shutdown, false if timed out.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/worker.clj#L241-L258">Source</a></sub></p>
