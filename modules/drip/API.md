# Table of contents
-  [`s-exp.drip`](#s-exp.drip)  - Drip: a Clojure job queue for MariaDB, PostgreSQL, and SQLite.
    -  [`cancel-job`](#s-exp.drip/cancel-job) - Cancels a job that is not already finalized.
    -  [`cancel-job!`](#s-exp.drip/cancel-job!) - Cancels a job that is not already finalized.
    -  [`complete-job`](#s-exp.drip/complete-job) - Marks a job as :completed.
    -  [`complete-job!`](#s-exp.drip/complete-job!) - Marks a job as :completed.
    -  [`constant-retry-policy`](#s-exp.drip/constant-retry-policy) - Returns a retry policy fn that always waits <code>delay</code> between retries.
    -  [`default-retention`](#s-exp.drip/default-retention)
    -  [`default-retry-policy`](#s-exp.drip/default-retry-policy) - Default retry policy fn.
    -  [`delete-job`](#s-exp.drip/delete-job) - Hard-deletes a job by ID regardless of state.
    -  [`delete-job!`](#s-exp.drip/delete-job!) - Hard-deletes a job by ID regardless of state.
    -  [`delete-jobs`](#s-exp.drip/delete-jobs) - Deletes jobs matching opts.
    -  [`delete-jobs!`](#s-exp.drip/delete-jobs!) - Deletes jobs matching opts.
    -  [`discard-job`](#s-exp.drip/discard-job) - Moves a job to :discarded state.
    -  [`discard-job!`](#s-exp.drip/discard-job!) - Moves a job to :discarded state.
    -  [`exponential-retry-policy`](#s-exp.drip/exponential-retry-policy) - Returns a retry policy fn with configurable exponential backoff.
    -  [`fetch-jobs`](#s-exp.drip/fetch-jobs) - Atomically claims up to limit available jobs from queue.
    -  [`fetch-jobs!`](#s-exp.drip/fetch-jobs!) - Atomically claims up to limit available jobs from queue using the supplied tx.
    -  [`get-job`](#s-exp.drip/get-job) - Returns a Job record by ID using the client's own datasource, or nil if not found.
    -  [`get-job!`](#s-exp.drip/get-job!) - Returns a Job record by ID using the supplied tx, or nil if not found.
    -  [`immediate-retry-policy`](#s-exp.drip/immediate-retry-policy) - Returns a retry policy fn that retries immediately with no delay.
    -  [`insert-job`](#s-exp.drip/insert-job) - Inserts a job using the client's own datasource.
    -  [`insert-job!`](#s-exp.drip/insert-job!) - Inserts a job using the supplied tx.
    -  [`insert-many`](#s-exp.drip/insert-many) - Inserts multiple jobs in a single transaction using the client's own datasource.
    -  [`insert-many!`](#s-exp.drip/insert-many!) - Inserts multiple jobs in a single transaction using the supplied tx.
    -  [`insert-many-fast!`](#s-exp.drip/insert-many-fast!) - PostgreSQL only.
    -  [`linear-retry-policy`](#s-exp.drip/linear-retry-policy) - Returns a retry policy fn that waits <code>base</code> * attempt.
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
    -  [`start-worker!`](#s-exp.drip/start-worker!) - Starts polling queues and processing jobs.
    -  [`start-periodic-jobs!`](#s-exp.drip/start-periodic-jobs!) - Schedules periodic job insertions.
    -  [`stop-and-cancel!`](#s-exp.drip/stop-and-cancel!) - Immediately interrupts all in-flight jobs and shuts down the worker.
    -  [`stop-worker!`](#s-exp.drip/stop-worker!) - Gracefully stops the worker.
    -  [`stop-periodic-jobs!`](#s-exp.drip/stop-periodic-jobs!) - Stops the periodic job scheduler.
    -  [`swap-job`](#s-exp.drip/swap-job) - Fetches a job by ID, applies f to it, and updates writable fields from the returned map.
    -  [`swap-job!`](#s-exp.drip/swap-job!) - Fetches a job by ID within the supplied tx, applies f to it, and updates writable fields from the returned map.
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
    -  [`migration-config`](#s-exp.drip.client/migration-config) - Returns a migratus config map (without :store and :db) for this client.
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
    -  [`constant-retry-policy`](#s-exp.drip.job/constant-retry-policy) - Returns a retry policy fn that always waits <code>delay</code> between retries.
    -  [`default-insert-opts`](#s-exp.drip.job/default-insert-opts)
    -  [`default-retry-policy`](#s-exp.drip.job/default-retry-policy) - Exponential backoff: base 1s, multiplier 2, max 1h, ±10% jitter.
    -  [`default-unique-states`](#s-exp.drip.job/default-unique-states)
    -  [`exponential-retry-policy`](#s-exp.drip.job/exponential-retry-policy) - Returns a retry policy fn with configurable exponential backoff.
    -  [`immediate-retry-policy`](#s-exp.drip.job/immediate-retry-policy) - Returns a retry policy fn that retries immediately with no delay.
    -  [`linear-retry-policy`](#s-exp.drip.job/linear-retry-policy) - Returns a retry policy fn that waits <code>base</code> * attempt.
    -  [`state->bit`](#s-exp.drip.job/state->bit)
    -  [`state-in-bitmask?`](#s-exp.drip.job/state-in-bitmask?) - Returns true if the given state keyword is represented in the bitmask.
    -  [`states`](#s-exp.drip.job/states)
    -  [`states->bitmask`](#s-exp.drip.job/states->bitmask) - Converts a collection of state keywords to an integer bitmask.
-  [`s-exp.drip.periodic`](#s-exp.drip.periodic) 
    -  [`start-periodic-jobs!`](#s-exp.drip.periodic/start-periodic-jobs!) - Schedules periodic job insertions for a sequence of spec maps.
    -  [`stop-periodic-jobs!`](#s-exp.drip.periodic/stop-periodic-jobs!) - Shuts down the periodic executor.
-  [`s-exp.drip.worker`](#s-exp.drip.worker) 
    -  [`start-worker!`](#s-exp.drip.worker/start-worker!) - Starts a job worker that polls queues and dispatches jobs to handlers.
    -  [`stop-and-cancel!`](#s-exp.drip.worker/stop-and-cancel!) - Immediately cancels all in-flight jobs by interrupting their threads, then shuts down.
    -  [`stop-worker!`](#s-exp.drip.worker/stop-worker!) - Gracefully shuts down the worker.

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

     ;; 3. Start the worker (registry maps kind strings to (fn [client job] ...) fns)
     (def worker
       (drip/start-worker!
         {:client client
          :registry {"send_email" (fn [_ job] (send-email! (:args job)))}
          :queues ["default" "priority"]}))

     ;; 4. Insert jobs
     (drip/insert-job client "send_email" {:to "user@example.com"})

     ;; 5. Stop on shutdown
     (drip/stop-worker! worker)




## <a name="s-exp.drip/cancel-job">`cancel-job`</a>
``` clojure

(cancel-job client job-id)
```
Function.

Cancels a job that is not already finalized. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L253-L257">Source</a></sub></p>

## <a name="s-exp.drip/cancel-job!">`cancel-job!`</a>
``` clojure

(cancel-job! client tx job-id)
```
Function.

Cancels a job that is not already finalized. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L259-L262">Source</a></sub></p>

## <a name="s-exp.drip/complete-job">`complete-job`</a>
``` clojure

(complete-job client job-id)
```
Function.

Marks a job as :completed. Uses the client's own datasource. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L242-L246">Source</a></sub></p>

## <a name="s-exp.drip/complete-job!">`complete-job!`</a>
``` clojure

(complete-job! client tx job-id)
```
Function.

Marks a job as :completed. Uses the supplied tx. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L248-L251">Source</a></sub></p>

## <a name="s-exp.drip/constant-retry-policy">`constant-retry-policy`</a>




Returns a retry policy fn that always waits `delay` between retries.
   `delay` is a duration value: string (e.g. "30s", "2m") or number of milliseconds.
   Options:
     :jitter - fractional ± jitter applied to delay (default 0.0)
   Example: (constant-retry-policy "30s" :jitter 0.1)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L463-L469">Source</a></sub></p>

## <a name="s-exp.drip/default-retention">`default-retention`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L497-L497">Source</a></sub></p>

## <a name="s-exp.drip/default-retry-policy">`default-retry-policy`</a>




Default retry policy fn. Takes attempt (1-based long), returns java.time.Instant.
   Exponential backoff: base 1s, multiplier 2, max 1h, ±10% jitter.
   Delays: ~1s, ~2s, ~4s, ~8s, ~16s, ~32s, ... capped at ~1h.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L457-L461">Source</a></sub></p>

## <a name="s-exp.drip/delete-job">`delete-job`</a>
``` clojure

(delete-job client job-id)
```
Function.

Hard-deletes a job by ID regardless of state. Uses the client's own datasource.
   Returns the deleted Job record, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L287-L292">Source</a></sub></p>

## <a name="s-exp.drip/delete-job!">`delete-job!`</a>
``` clojure

(delete-job! client tx job-id)
```
Function.

Hard-deletes a job by ID regardless of state. Uses the supplied tx.
   Returns the deleted Job record, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L294-L298">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L335-L347">Source</a></sub></p>

## <a name="s-exp.drip/delete-jobs!">`delete-jobs!`</a>
``` clojure

(delete-jobs! client tx opts)
```
Function.

Deletes jobs matching opts. Uses the supplied tx.
   Returns count of deleted jobs. See delete-jobs for opts documentation.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L349-L353">Source</a></sub></p>

## <a name="s-exp.drip/discard-job">`discard-job`</a>
``` clojure

(discard-job client job-id)
```
Function.

Moves a job to :discarded state. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L276-L280">Source</a></sub></p>

## <a name="s-exp.drip/discard-job!">`discard-job!`</a>
``` clojure

(discard-job! client tx job-id)
```
Function.

Moves a job to :discarded state. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L282-L285">Source</a></sub></p>

## <a name="s-exp.drip/exponential-retry-policy">`exponential-retry-policy`</a>




Returns a retry policy fn with configurable exponential backoff.
   Waits `base` * multiplier^(attempt-1), capped at `max`.
   `base` is a duration value: string (e.g. "1s") or number of milliseconds.
   Options:
     :multiplier - growth factor (default 2.0)
     :max        - duration cap on computed delay (default "1h")
     :jitter     - fractional ± jitter applied to delay (default 0.1)
   Example: (exponential-retry-policy "1s" :multiplier 2.0 :max "30m" :jitter 0.15)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L480-L489">Source</a></sub></p>

## <a name="s-exp.drip/fetch-jobs">`fetch-jobs`</a>
``` clojure

(fetch-jobs client queue worker-id & {:as opts})
```
Function.

Atomically claims up to limit available jobs from queue.
   Opens its own transaction. Returns vector of Job records.
   opts: :limit (default 10)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L136-L142">Source</a></sub></p>

## <a name="s-exp.drip/fetch-jobs!">`fetch-jobs!`</a>
``` clojure

(fetch-jobs! client tx queue worker-id opts)
```
Function.

Atomically claims up to limit available jobs from queue using the supplied tx.
   Returns vector of Job records.
   opts: :limit (default 10)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L144-L149">Source</a></sub></p>

## <a name="s-exp.drip/get-job">`get-job`</a>
``` clojure

(get-job client job-id)
```
Function.

Returns a Job record by ID using the client's own datasource, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L151-L155">Source</a></sub></p>

## <a name="s-exp.drip/get-job!">`get-job!`</a>
``` clojure

(get-job! client tx job-id)
```
Function.

Returns a Job record by ID using the supplied tx, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L157-L160">Source</a></sub></p>

## <a name="s-exp.drip/immediate-retry-policy">`immediate-retry-policy`</a>




Returns a retry policy fn that retries immediately with no delay.
   Useful for tests or jobs that should be reattempted without waiting.
   (immediate-retry-policy)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L491-L495">Source</a></sub></p>

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

   Key opts:
     :queue        - queue name (default "default")
     :priority     - 1–4 (default 1, lower = higher priority)
     :max-attempts - max retry attempts (default 25)
     :scheduled-at - java.time.Instant to delay execution until
     :tags         - vector of string tags
     :metadata     - map of extra metadata
     :unique-opts  - map for unique job constraints
     :ephemeral    - if true, job is deleted immediately on successful completion
                     instead of transitioning to :completed. Failures behave normally.
                     Default: false
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L63-L86">Source</a></sub></p>

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

   See insert-job for full opts documentation including :ephemeral.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L88-L98">Source</a></sub></p>

## <a name="s-exp.drip/insert-many">`insert-many`</a>
``` clojure

(insert-many client job-specs)
```
Function.

Inserts multiple jobs in a single transaction using the client's own datasource.
   `job-specs` - sequence of [kind args opts] tuples.
   Returns a vector of Job records.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L100-L110">Source</a></sub></p>

## <a name="s-exp.drip/insert-many!">`insert-many!`</a>
``` clojure

(insert-many! client tx job-specs)
```
Function.

Inserts multiple jobs in a single transaction using the supplied tx.
   `job-specs` - sequence of [kind args opts] tuples.
   Returns a vector of Job records.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L112-L118">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L120-L134">Source</a></sub></p>

## <a name="s-exp.drip/linear-retry-policy">`linear-retry-policy`</a>




Returns a retry policy fn that waits `base` * attempt.
   `base` is a duration value: string (e.g. "10s") or number of milliseconds.
   Options:
     :max    - duration cap on computed delay (default unbounded)
     :jitter - fractional ± jitter applied to delay (default 0.0)
   Example: (linear-retry-policy "10s" :max "5m" :jitter 0.1)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L471-L478">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L162-L180">Source</a></sub></p>

## <a name="s-exp.drip/list-jobs!">`list-jobs!`</a>
``` clojure

(list-jobs! client tx opts)
```
Function.

Lists jobs with optional filters. Uses the supplied tx.
   See list-jobs for opts documentation.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L182-L186">Source</a></sub></p>

## <a name="s-exp.drip/list-queues">`list-queues`</a>
``` clojure

(list-queues client)
```
Function.

Returns all queues. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L392-L396">Source</a></sub></p>

## <a name="s-exp.drip/list-queues!">`list-queues!`</a>
``` clojure

(list-queues! client tx)
```
Function.

Returns all queues. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L398-L401">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L370-L374">Source</a></sub></p>

## <a name="s-exp.drip/pause-queue!">`pause-queue!`</a>
``` clojure

(pause-queue! client tx queue-name)
```
Function.

Pauses a queue - workers stop fetching from it. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L376-L379">Source</a></sub></p>

## <a name="s-exp.drip/record-output">`record-output`</a>
``` clojure

(record-output client job-id output)
```
Function.

Merges {:output output} into the job's metadata column.
   Uses the client's own datasource. Returns updated Job record.
   Output is accessible as (get-in job [:metadata "output"]) after completion.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L228-L234">Source</a></sub></p>

## <a name="s-exp.drip/record-output!">`record-output!`</a>
``` clojure

(record-output! client tx job-id output)
```
Function.

Merges {:output output} into the job's metadata column. Uses the supplied tx.
   Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L236-L240">Source</a></sub></p>

## <a name="s-exp.drip/rescue-stuck-jobs">`rescue-stuck-jobs`</a>
``` clojure

(rescue-stuck-jobs client stuck-after retry-policy)
(rescue-stuck-jobs client stuck-after retry-policy queues)
```
Function.

Rescues jobs stuck in :running state with attempted_at <= stuck-after.
   Transitions each to :retryable or :discarded (based on attempt vs max-attempts).
   Uses the client's own datasource. Returns count of rescued jobs.
   queues - optional seq of queue names to restrict rescue to; nil = all queues.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L315-L324">Source</a></sub></p>

## <a name="s-exp.drip/rescue-stuck-jobs!">`rescue-stuck-jobs!`</a>
``` clojure

(rescue-stuck-jobs! client tx stuck-after retry-policy)
(rescue-stuck-jobs! client tx stuck-after retry-policy queues)
```
Function.

Rescues jobs stuck in :running state with attempted_at <= stuck-after.
   Uses the supplied tx. Returns count of rescued jobs.
   queues - optional seq of queue names to restrict rescue to; nil = all queues.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L326-L333">Source</a></sub></p>

## <a name="s-exp.drip/resume-queue">`resume-queue`</a>
``` clojure

(resume-queue client queue-name)
```
Function.

Resumes a paused queue. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L381-L385">Source</a></sub></p>

## <a name="s-exp.drip/resume-queue!">`resume-queue!`</a>
``` clojure

(resume-queue! client tx queue-name)
```
Function.

Resumes a paused queue. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L387-L390">Source</a></sub></p>

## <a name="s-exp.drip/retry-job">`retry-job`</a>
``` clojure

(retry-job client job-id)
```
Function.

Forces a failed/cancelled/discarded job back to :available.
   Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L264-L269">Source</a></sub></p>

## <a name="s-exp.drip/retry-job!">`retry-job!`</a>
``` clojure

(retry-job! client tx job-id)
```
Function.

Forces a failed/cancelled/discarded job back to :available. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L271-L274">Source</a></sub></p>

## <a name="s-exp.drip/snooze-job">`snooze-job`</a>
``` clojure

(snooze-job client job-id duration)
```
Function.

Reschedules a :running job to run again after duration without consuming a retry attempt.
   duration - ms number or duration string (e.g. "1h", "30m", "90s").
   Uses the client's own datasource. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L300-L306">Source</a></sub></p>

## <a name="s-exp.drip/snooze-job!">`snooze-job!`</a>
``` clojure

(snooze-job! client tx job-id duration)
```
Function.

Reschedules a :running job to run again after duration without consuming a retry attempt.
   duration - ms number or duration string (e.g. "1h", "30m", "90s").
   Uses the supplied tx. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L308-L313">Source</a></sub></p>

## <a name="s-exp.drip/start-worker!">`start-worker!`</a>
``` clojure

(start-worker! opts)
```
Function.

Starts polling queues and processing jobs.
   :client and :registry are required. See s-exp.drip.worker/start-worker! for full option docs.

   Registry handlers receive [client job] as two arguments.
   Handlers must explicitly call complete-job!, snooze-job!, etc. to manage job state.
   Throwing any Throwable signals failure and triggers retry or discard.

   Key options:
     :retry-policies - {:default policy-fn, "kind" policy-fn} unified retry map
     :job-timeouts   - {:default timeout, "kind" timeout} unified timeout map; duration string or ms (nil = no timeout)
     :rescue-after   - {:default duration, "queue" duration} unified rescue map (nil or :default nil = disable)
     :retention      - {:default {state → ms}, "queue" {state → ms}} unified retention map
                       Set :retention nil to disable all cleanup.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L407-L422">Source</a></sub></p>

## <a name="s-exp.drip/start-periodic-jobs!">`start-periodic-jobs!`</a>
``` clojure

(start-periodic-jobs! client specs)
```
Function.

Schedules periodic job insertions. `specs` is a sequence of PeriodicSpec maps.
   Returns a scheduler; stop with stop-periodic-jobs!.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L442-L446">Source</a></sub></p>

## <a name="s-exp.drip/stop-and-cancel!">`stop-and-cancel!`</a>
``` clojure

(stop-and-cancel! worker)
```
Function.

Immediately interrupts all in-flight jobs and shuts down the worker.
   In-flight jobs remain in :running state; rescue-stuck-jobs will requeue them.
   Use stop-worker! instead when you want to wait for jobs to finish gracefully.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L431-L436">Source</a></sub></p>

## <a name="s-exp.drip/stop-worker!">`stop-worker!`</a>
``` clojure

(stop-worker! worker)
(stop-worker! worker)
```
Function.

Gracefully stops the worker. Accepts :timeout (duration string or ms, default "30s") and :drain keyword args.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L424-L429">Source</a></sub></p>

## <a name="s-exp.drip/stop-periodic-jobs!">`stop-periodic-jobs!`</a>
``` clojure

(stop-periodic-jobs! scheduler)
```
Function.

Stops the periodic job scheduler.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L448-L451">Source</a></sub></p>

## <a name="s-exp.drip/swap-job">`swap-job`</a>
``` clojure

(swap-job client job-id f)
```
Function.

Fetches a job by ID, applies f to it, and updates writable fields from the
   returned map. f receives the Job record and must return a map with any subset
   of update-job keys:
     :metadata :priority :queue :scheduled-at :state :max-attempts :tags
   Uses the client's own datasource. Returns the updated Job record, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L218-L226">Source</a></sub></p>

## <a name="s-exp.drip/swap-job!">`swap-job!`</a>
``` clojure

(swap-job! client tx job-id f)
```
Function.

Fetches a job by ID within the supplied tx, applies f to it, and updates
   writable fields from the returned map. f receives the Job record and must
   return a map with any subset of update-job keys:
     :metadata :priority :queue :scheduled-at :state :max-attempts :tags
   Returns the updated Job record, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L208-L216">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L188-L200">Source</a></sub></p>

## <a name="s-exp.drip/update-job!">`update-job!`</a>
``` clojure

(update-job! client tx job-id opts)
```
Function.

Updates writable fields of a job. Uses the supplied tx. Returns updated Job record.
   See update-job for opts documentation.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L202-L206">Source</a></sub></p>

## <a name="s-exp.drip/upsert-queue">`upsert-queue`</a>
``` clojure

(upsert-queue client queue-name metadata)
```
Function.

Creates or updates a queue. Uses the client's own datasource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L359-L363">Source</a></sub></p>

## <a name="s-exp.drip/upsert-queue!">`upsert-queue!`</a>
``` clojure

(upsert-queue! client tx queue-name metadata)
```
Function.

Creates or updates a queue. Uses the supplied tx.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip.clj#L365-L368">Source</a></sub></p>

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



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L26-L94">Source</a></sub></p>

## <a name="s-exp.drip.client/migration">`Migration`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L12-L14">Source</a></sub></p>

## <a name="s-exp.drip.client/notifications">`Notifications`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L16-L24">Source</a></sub></p>

## <a name="s-exp.drip.client/queues">`Queues`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L96-L106">Source</a></sub></p>

## <a name="s-exp.drip.client/cancel-job!">`cancel-job!`</a>
``` clojure

(cancel-job! client tx job-id)
```
Function.

Cancels a non-finalized job. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L39-L40">Source</a></sub></p>

## <a name="s-exp.drip.client/complete-job!">`complete-job!`</a>
``` clojure

(complete-job! client tx job-id)
```
Function.

Marks job as :completed. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L35-L36">Source</a></sub></p>

## <a name="s-exp.drip.client/delete-job!">`delete-job!`</a>
``` clojure

(delete-job! client tx job-id)
```
Function.

Hard-deletes a single job by ID regardless of state.
     Returns the deleted Job record, or nil if not found.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L74-L76">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L55-L63">Source</a></sub></p>

## <a name="s-exp.drip.client/discard-job!">`discard-job!`</a>
``` clojure

(discard-job! client tx job-id)
```
Function.

Moves job to :discarded. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L43-L44">Source</a></sub></p>

## <a name="s-exp.drip.client/fail-job!">`fail-job!`</a>
``` clojure

(fail-job! client tx job-id error-map retry-policy)
```
Function.

Records error, transitions to :retryable or :discarded. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L37-L38">Source</a></sub></p>

## <a name="s-exp.drip.client/fetch-jobs!">`fetch-jobs!`</a>
``` clojure

(fetch-jobs! client tx queue worker-id opts)
```
Function.

Atomically claims up to limit available jobs using the supplied tx.
     opts: :limit (default 10). Returns vector of Job records.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L29-L31">Source</a></sub></p>

## <a name="s-exp.drip.client/get-job!">`get-job!`</a>
``` clojure

(get-job! client tx job-id)
```
Function.

Fetches single job by ID. Returns Job record or nil.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L77-L78">Source</a></sub></p>

## <a name="s-exp.drip.client/insert-job!">`insert-job!`</a>
``` clojure

(insert-job! client tx kind args opts)
```
Function.

Inserts a job using tx. Returns a Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L27-L28">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L79-L94">Source</a></sub></p>

## <a name="s-exp.drip.client/list-queues!">`list-queues!`</a>
``` clojure

(list-queues! client tx)
```
Function.

Returns all queues as a vector of maps.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L105-L106">Source</a></sub></p>

## <a name="s-exp.drip.client/migration-config">`migration-config`</a>
``` clojure

(migration-config client)
```
Function.

Returns a migratus config map (without :store and :db) for this client.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L13-L14">Source</a></sub></p>

## <a name="s-exp.drip.client/notify-job-available!">`notify-job-available!`</a>
``` clojure

(notify-job-available! client queue)
```
Function.

Notifies listeners that jobs are available in queue.
     No-op on MariaDB/SQLite; calls pg_notify on PostgreSQL.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L17-L19">Source</a></sub></p>

## <a name="s-exp.drip.client/pause-queue!">`pause-queue!`</a>
``` clojure

(pause-queue! client tx queue-name)
```
Function.

Pauses a queue.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L99-L100">Source</a></sub></p>

## <a name="s-exp.drip.client/promote-scheduled-jobs!">`promote-scheduled-jobs!`</a>
``` clojure

(promote-scheduled-jobs! client tx)
```
Function.

Moves :scheduled/:retryable jobs whose scheduled_at <= now to :available.
     Returns count of promoted jobs.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L48-L50">Source</a></sub></p>

## <a name="s-exp.drip.client/queue-paused?">`queue-paused?`</a>
``` clojure

(queue-paused? client tx queue-name)
```
Function.

Returns true if queue exists and is paused.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L103-L104">Source</a></sub></p>

## <a name="s-exp.drip.client/record-output!">`record-output!`</a>
``` clojure

(record-output! client tx job-id output)
```
Function.

Merges {:output output} into the job's metadata column. Returns updated Job record.
     Output is accessible as (get-in job [:metadata "output"]) after completion.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L32-L34">Source</a></sub></p>

## <a name="s-exp.drip.client/rescue-stuck-jobs!">`rescue-stuck-jobs!`</a>
``` clojure

(rescue-stuck-jobs! client tx stuck-after retry-policy queues)
```
Function.

Moves :running jobs with attempted_at <= stuck-after to :retryable or :discarded.
     Appends a rescue error entry to each job. Returns count of rescued jobs.
     queues - optional seq of queue names to restrict rescue to; nil = all queues.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L51-L54">Source</a></sub></p>

## <a name="s-exp.drip.client/resume-queue!">`resume-queue!`</a>
``` clojure

(resume-queue! client tx queue-name)
```
Function.

Resumes a paused queue.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L101-L102">Source</a></sub></p>

## <a name="s-exp.drip.client/retry-job!">`retry-job!`</a>
``` clojure

(retry-job! client tx job-id)
```
Function.

Forces job back to :available. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L41-L42">Source</a></sub></p>

## <a name="s-exp.drip.client/snooze-job!">`snooze-job!`</a>
``` clojure

(snooze-job! client tx job-id duration)
```
Function.

Reschedules a :running job to run again after duration (ms number or duration string e.g. "1h")
     without consuming a retry attempt. Returns updated Job record.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L45-L47">Source</a></sub></p>

## <a name="s-exp.drip.client/start-listener!">`start-listener!`</a>
``` clojure

(start-listener! client on-notify)
```
Function.

Starts background LISTEN connection, calls (on-notify queue) on each
     notification. Returns opaque listener handle (nil if unsupported).
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L20-L22">Source</a></sub></p>

## <a name="s-exp.drip.client/stop-listener!">`stop-listener!`</a>
``` clojure

(stop-listener! client listener)
```
Function.

Stops listener returned by start-listener!. No-op if listener is nil.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L23-L24">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L64-L73">Source</a></sub></p>

## <a name="s-exp.drip.client/upsert-queue!">`upsert-queue!`</a>
``` clojure

(upsert-queue! client tx queue-name metadata)
```
Function.

Creates or updates a queue.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client.clj#L97-L98">Source</a></sub></p>

-----
# <a name="s-exp.drip.client.mariadb">s-exp.drip.client.mariadb</a>






## <a name="s-exp.drip.client.mariadb/make-client">`make-client`</a>
``` clojure

(make-client ds)
```
Function.

Returns a MariaDB client. `ds` is a javax.sql.DataSource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client/mariadb.clj#L417-L420">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client/postgres.clj#L552-L568">Source</a></sub></p>

## <a name="s-exp.drip.client.postgres/make-client">`make-client`</a>
``` clojure

(make-client ds)
```
Function.

Returns a PostgreSQL client. `ds` is a javax.sql.DataSource.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client/postgres.clj#L570-L573">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/client/sqlite.clj#L421-L429">Source</a></sub></p>

-----
# <a name="s-exp.drip.db">s-exp.drip.db</a>






## <a name="s-exp.drip.db/->json">`->json`</a>
``` clojure

(->json v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L39-L40">Source</a></sub></p>

## <a name="s-exp.drip.db/->json-str">`->json-str`</a>
``` clojure

(->json-str v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L42-L43">Source</a></sub></p>

## <a name="s-exp.drip.db/<-json">`<-json`</a>
``` clojure

(<-json v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L56-L57">Source</a></sub></p>

## <a name="s-exp.drip.db/<-json-metadata">`<-json-metadata`</a>
``` clojure

(<-json-metadata v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L59-L61">Source</a></sub></p>

## <a name="s-exp.drip.db/compute-unique-key">`compute-unique-key`</a>
``` clojure

(compute-unique-key kind encoded-args queue now unique-opts)
```
Function.

Computes a 32-byte SHA-256 unique key for a job.
   Returns nil when unique-opts is nil (no uniqueness constraint).
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L121-L140">Source</a></sub></p>

## <a name="s-exp.drip.db/instant->str">`instant->str`</a>
``` clojure

(instant->str i)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L88-L89">Source</a></sub></p>

## <a name="s-exp.drip.db/instant->ts">`instant->ts`</a>
``` clojure

(instant->ts i)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L85-L86">Source</a></sub></p>

## <a name="s-exp.drip.db/jdbc-opts">`jdbc-opts`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L95-L96">Source</a></sub></p>

## <a name="s-exp.drip.db/mapper">`mapper`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L19-L27">Source</a></sub></p>

## <a name="s-exp.drip.db/migrate!">`migrate!`</a>
``` clojure

(migrate! c)
```
Function.

Runs pending migrations against the datasource. Idempotent - safe to call
   on every application startup. Accepts a Client record (from make-client).
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L146-L152">Source</a></sub></p>

## <a name="s-exp.drip.db/ts->instant">`ts->instant`</a>
``` clojure

(ts->instant v)
```
Function.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L75-L83">Source</a></sub></p>

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
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/db.clj#L98-L108">Source</a></sub></p>

-----
# <a name="s-exp.drip.job">s-exp.drip.job</a>






## <a name="s-exp.drip.job/bitmask->states">`bitmask->states`</a>
``` clojure

(bitmask->states mask)
```
Function.

Converts an integer bitmask to a set of state keywords.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L32-L40">Source</a></sub></p>

## <a name="s-exp.drip.job/constant-retry-policy">`constant-retry-policy`</a>
``` clojure

(constant-retry-policy delay & {:keys [jitter], :or {jitter 0.0}})
```
Function.

Returns a retry policy fn that always waits `delay` between retries.
   `delay` is a duration value: string (e.g. "30s", "2m") or number of milliseconds.
   Options:
     :jitter - fractional ± jitter applied to delay (default 0.0)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L69-L77">Source</a></sub></p>

## <a name="s-exp.drip.job/default-insert-opts">`default-insert-opts`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L51-L57">Source</a></sub></p>

## <a name="s-exp.drip.job/default-retry-policy">`default-retry-policy`</a>




Exponential backoff: base 1s, multiplier 2, max 1h, ±10% jitter.
   Delays: ~1s, ~2s, ~4s, ~8s, ~16s, ~32s, ... capped at ~1h.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L107-L110">Source</a></sub></p>

## <a name="s-exp.drip.job/default-unique-states">`default-unique-states`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L48-L49">Source</a></sub></p>

## <a name="s-exp.drip.job/exponential-retry-policy">`exponential-retry-policy`</a>
``` clojure

(exponential-retry-policy base & {:keys [multiplier max jitter], :or {multiplier 2.0, max "1h", jitter 0.1}})
```
Function.

Returns a retry policy fn with configurable exponential backoff.
   Waits `base` * multiplier^(attempt-1), capped at `max`.
   `base` is a duration value: string (e.g. "1s") or number of milliseconds.
   Options:
     :multiplier - growth factor (default 2.0)
     :max        - duration cap on computed delay (default "1h")
     :jitter     - fractional ± jitter applied to delay (default 0.1)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L91-L105">Source</a></sub></p>

## <a name="s-exp.drip.job/immediate-retry-policy">`immediate-retry-policy`</a>
``` clojure

(immediate-retry-policy)
```
Function.

Returns a retry policy fn that retries immediately with no delay.
   Useful for testing or jobs that should be reattempted without waiting.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L112-L116">Source</a></sub></p>

## <a name="s-exp.drip.job/linear-retry-policy">`linear-retry-policy`</a>
``` clojure

(linear-retry-policy base & {:keys [max jitter], :or {max Long/MAX_VALUE, jitter 0.0}})
```
Function.

Returns a retry policy fn that waits `base` * attempt.
   `base` is a duration value: string (e.g. "10s") or number of milliseconds.
   Options:
     :max    - duration cap on computed delay (default unbounded)
     :jitter - fractional ± jitter applied to delay (default 0.0)
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L79-L89">Source</a></sub></p>

## <a name="s-exp.drip.job/state->bit">`state->bit`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L12-L20">Source</a></sub></p>

## <a name="s-exp.drip.job/state-in-bitmask?">`state-in-bitmask?`</a>
``` clojure

(state-in-bitmask? state mask)
```
Function.

Returns true if the given state keyword is represented in the bitmask.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L42-L45">Source</a></sub></p>

## <a name="s-exp.drip.job/states">`states`</a>



<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L22-L22">Source</a></sub></p>

## <a name="s-exp.drip.job/states->bitmask">`states->bitmask`</a>
``` clojure

(states->bitmask state-coll)
```
Function.

Converts a collection of state keywords to an integer bitmask.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/job.clj#L24-L30">Source</a></sub></p>

-----
# <a name="s-exp.drip.periodic">s-exp.drip.periodic</a>






## <a name="s-exp.drip.periodic/start-periodic-jobs!">`start-periodic-jobs!`</a>
``` clojure

(start-periodic-jobs! c specs)
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
   Returns a ScheduledExecutorService. Stop with stop-periodic-jobs!.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/periodic.clj#L24-L58">Source</a></sub></p>

## <a name="s-exp.drip.periodic/stop-periodic-jobs!">`stop-periodic-jobs!`</a>
``` clojure

(stop-periodic-jobs! scheduler)
```
Function.

Shuts down the periodic executor.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/periodic.clj#L60-L63">Source</a></sub></p>

-----
# <a name="s-exp.drip.worker">s-exp.drip.worker</a>






## <a name="s-exp.drip.worker/start-worker!">`start-worker!`</a>
``` clojure

(start-worker!
 {:keys
  [client registry retry-policies job-timeouts queues concurrency poll-interval worker-id],
  :or
  {queues ["default"],
   concurrency 10,
   poll-interval "1s",
   retry-policies {:default job/default-retry-policy},
   job-timeouts {:default nil}}})
```
Function.

Starts a job worker that polls queues and dispatches jobs to handlers.

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
     :poll-interval  - polling interval; duration string or ms (default "1s")
     :worker-id         - unique string ID (default random UUID)
     :retry-policies    - {kind-string retry-policy-fn, :default retry-policy-fn} map.
                          :default is the fallback policy (default: exponential backoff).
                          kind-string entries override :default for that job kind.
                          Policy fn: (fn [attempt] java.time.Instant)
                          Example: {:default drip/default-retry-policy
                                    "email" fast-retry-policy}
     :job-timeouts      - unified timeout config map. :default is the global timeout as a
                          duration string or ms (nil = no timeout); kind-string keys override per kind.
                          Default: {:default nil} (no timeout).
                          Example: {:default "30s"
                                    "slow_report" "2m"
                                    "quick_notify" "5s"}
     :event-fn          - optional (fn [event]) called for every worker event.
                          Exceptions are swallowed — never affects job processing.
                          Event types: :s-exp.drip.job/start :s-exp.drip.job/complete
                                       :s-exp.drip.job/fail :s-exp.drip.job/timeout
                                       :s-exp.drip.job/discard :s-exp.drip.poll/fetched
                          All events carry :worker-id :queue :kind :job-id :attempt where applicable.
                          complete/fail/timeout add :duration-ms; fail/timeout add :error (Throwable).

   On PostgreSQL, a LISTEN connection is started automatically; inserts from
   other processes trigger an immediate poll instead of waiting for the interval.

   Returns a Worker record. Stop with stop-worker!.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/worker.clj#L163-L273">Source</a></sub></p>

## <a name="s-exp.drip.worker/stop-and-cancel!">`stop-and-cancel!`</a>
``` clojure

(stop-and-cancel! {:keys [client task-executor scheduler listener running?]})
```
Function.

Immediately cancels all in-flight jobs by interrupting their threads, then shuts down.
   In-flight jobs remain in :running state and will be rescued by rescue-stuck-jobs on the
   next worker startup (or via the periodic rescue in another running worker).
   Returns the list of cancelled Futures from shutdownNow.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/worker.clj#L294-L306">Source</a></sub></p>

## <a name="s-exp.drip.worker/stop-worker!">`stop-worker!`</a>
``` clojure

(stop-worker! worker)
(stop-worker! worker & {:keys [timeout drain]})
```
Function.

Gracefully shuts down the worker.
   Waits up to :timeout for in-flight jobs to finish (default "30s").
   Returns true if clean shutdown, false if timed out.
<p><sub><a href="https://github.com/mpenet/drip/blob/main/modules/drip/src/s_exp/drip/worker.clj#L275-L292">Source</a></sub></p>
