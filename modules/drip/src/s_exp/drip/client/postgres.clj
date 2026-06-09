(ns s-exp.drip.client.postgres
  "PostgreSQL client."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [s-exp.drip.client :as client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.duration :as duration])
  (:import (java.io ByteArrayInputStream)
           (java.nio.charset StandardCharsets)
           (java.sql Array Connection)
           (java.time Instant)
           (org.postgresql PGConnection)
           (org.postgresql.copy CopyManager)))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; LISTEN/NOTIFY
;; ---------------------------------------------------------------------------

(defrecord PGListener [^Connection conn ^Thread thread running?])

(defn- pg-conn ^PGConnection [^Connection conn]
  (.unwrap conn PGConnection))

(defn- start-pg-listener! [ds on-notify poll-ms]
  (let [conn (.getConnection ^javax.sql.DataSource ds)
        _ (try
            (with-open [stmt (.createStatement conn)]
              (.execute stmt "LISTEN drip_insert"))
            (catch Exception e
              (.close conn)
              (throw e)))
        running? (atom true)]
    (let [thread (Thread.
                  ^Runnable
                  (fn []
                    (while @running?
                      (try
                        (let [notifications (.getNotifications (pg-conn conn))]
                          (when notifications
                            (doseq [n notifications]
                              (on-notify (.getParameter ^org.postgresql.core.Notification n)))))
                        (when @running?
                          (Thread/sleep (long poll-ms)))
                        (catch InterruptedException _
                          (reset! running? false))
                        (catch Exception t
                          (when @running? (log/error t "drip: pg-listener error")))))))]
      (.setDaemon thread true)
      (.setName thread "drip-pg-listener")
      (.start thread)
      (->PGListener conn thread running?))))

(defn- stop-pg-listener! [^PGListener listener]
  (reset! (:running? listener) false)
  (.interrupt ^Thread (:thread listener))
  (try (.close ^Connection (:conn listener)) (catch Exception _ nil)))

;; ---------------------------------------------------------------------------
;; River BIT(8) encoding
;;
;; PostgreSQL stores unique_states as BIT(8), MSB first.
;; Bit positions are taken from job/state->bit (shared with MariaDB/SQLite).
;; ---------------------------------------------------------------------------

(defn- states->bitmask-str ^String [state-coll]
  (let [bits (int-array 8)]
    (doseq [s state-coll]
      (when-let [pos (job/state->bit s)]
        (aset bits pos (int 1))))
    (apply str (map #(aget bits %) (range 8)))))

;; ---------------------------------------------------------------------------
;; Row coercion
;; ---------------------------------------------------------------------------

(defn- pg-array->vec [^Array arr]
  (when arr (vec (.getArray arr))))

(defn- pg-jsonb-array->vec [^Array arr]
  (when arr (mapv db/<-json (pg-array->vec arr))))

(defn- row->job [row]
  (when row
    {:id (:id row)
     :attempt (:attempt row)
     :attempted-at (db/ts->instant (:attempted-at row))
     :attempted-by (pg-array->vec (:attempted-by row))
     :created-at (db/ts->instant (:created-at row))
     :args (db/<-json (:args row))
     :errors (pg-jsonb-array->vec (:errors row))
     :finalized-at (db/ts->instant (:finalized-at row))
     :kind (:kind row)
     :max-attempts (:max-attempts row)
     :metadata (db/<-json-metadata (:metadata row))
     :priority (:priority row)
     :queue (:queue row)
     :scheduled-at (db/ts->instant (:scheduled-at row))
     :state (keyword (:state row))
     :tags (pg-array->vec (:tags row))
     :unique-key (:unique-key row)
     :unique-states (:unique-states row)
     :ephemeral (boolean (:ephemeral row))
     :timeout-ms (:timeout-ms row)}))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- in-clause [ids]
  [(str "(" (str/join "," (repeat (count ids) "?")) ")")
   ids])

(defn- encode-ts ^java.sql.Timestamp [^Instant i]
  (db/instant->ts i))

;; ---------------------------------------------------------------------------
;; COPY-based fast batch insert helpers
;;
;; Uses PostgreSQL COPY FROM STDIN (text format) for high-throughput insertion.
;; No RETURNING — caller gets the inserted count, not Job records.
;; Does NOT support unique_key deduplication (same limitation as River's InsertManyFast).
;;
;; Text format rules:
;;   - columns tab-separated
;;   - rows newline-terminated (\n)
;;   - NULL represented as \N
;;   - literal tab/newline/backslash in values must be escaped: \t \n \\
;; ---------------------------------------------------------------------------

(defn- copy-escape ^String [^String s]
  (when s
    (-> s
        (.replace "\\" "\\\\")
        (.replace "\t" "\\t")
        (.replace "\n" "\\n")
        (.replace "\r" "\\r"))))

(defn- append-copy-row [^StringBuilder sb kind args opts ^Instant now]
  (let [opts (merge job/default-insert-opts opts)
        queue (:queue opts)
        scheduled-at (or (:scheduled-at opts) now)
        initial-state (if (.isAfter ^Instant scheduled-at now) "scheduled" "available")]
    (doto sb
      (.append initial-state) (.append \tab)
      (.append "0") (.append \tab)
      (.append (int (:max-attempts opts))) (.append \tab)
      (.append (str (encode-ts scheduled-at))) (.append \tab)
      (.append (int (:priority opts))) (.append \tab)
      (.append (copy-escape (db/->json-str args))) (.append \tab)
      (.append "{}") (.append \tab) ; attempted_by (text[])
      (.append "{}") (.append \tab) ; errors (jsonb[])
      (.append (copy-escape kind)) (.append \tab)
      (.append (copy-escape (db/->json-str (:metadata opts)))) (.append \tab)
      (.append (copy-escape queue)) (.append \tab)
      (.append "{}") ; tags (text[])
      (.append \newline))))

;; ---------------------------------------------------------------------------
;; Client record
;; ---------------------------------------------------------------------------

(defrecord PostgresClient [ds]
  client/Migration
  (migration-config [_]
    {:migration-dir "migrations/postgres"
     :migration-table-name "drip_migration"})

  client/Notifications
  (notify-job-available! [_ queue]
    (jdbc/execute-one!
     ds
     ["SELECT pg_notify('drip_insert', ?)" (db/->json-str {:queue queue})]))
  (start-listener! [_ on-notify]
    (start-pg-listener!
     ds
     (fn [payload]
       (try
         (let [m (db/<-json payload)]
           (on-notify (:queue m)))
         (catch Exception _ nil)))
     500))
  (stop-listener! [_ listener]
    (when listener (stop-pg-listener! listener)))

  client/Jobs
  (insert-job! [_ tx kind args opts]
    (let [opts (merge job/default-insert-opts opts)
          now (Instant/now)
          queue (:queue opts)
          encoded-args (db/->json args)
          unique-opts (:unique-opts opts)
          unique-key (db/compute-unique-key kind args queue now unique-opts)
          unique-states-str (when unique-opts
                              (states->bitmask-str
                               (or (:by-state unique-opts) job/default-unique-states)))
          scheduled-at (or (:scheduled-at opts) now)
          initial-state (if (.isAfter ^Instant scheduled-at now) "scheduled" "available")
          tags-arr ^"[Ljava.lang.String;" (into-array String (map str (:tags opts)))
          timeout-ms (when-let [t (:timeout opts)] (long (duration/duration t)))
          result (try
                   (jdbc/execute-one!
                    tx
                    ["INSERT INTO drip_job
                        (state, attempt, max_attempts, scheduled_at, priority,
                         args, attempted_by, errors,
                         kind, metadata, queue, tags,
                         unique_key, unique_states, ephemeral, timeout_ms)
                      VALUES (?::drip_job_state, 0, ?, ?, ?,
                              ?::jsonb, '{}', '{}',
                              ?, ?::jsonb, ?, ?,
                              ?, ?::bit(8), ?, ?)
                      RETURNING *"
                     initial-state
                     (int (:max-attempts opts))
                     (encode-ts scheduled-at)
                     (int (:priority opts))
                     (db/->json-str args)
                     kind
                     (db/->json-str (:metadata opts))
                     queue
                     tags-arr
                     unique-key
                     unique-states-str
                     (boolean (:ephemeral opts))
                     timeout-ms]
                    db/jdbc-opts)
                   (catch java.sql.SQLException e
                     (when-not (db/unique-conflict-sql-states (.getSQLState e))
                       (throw e))
                     (throw (db/unique-conflict-ex kind queue unique-opts e))))]
      (row->job result)))

  (fetch-jobs! [_ tx queue worker-id opts]
    (let [{:keys [limit] :or {limit 10}} opts
          now (Instant/now)
          rows (jdbc/execute!
                tx
                ["SELECT * FROM drip_job
                    WHERE state = 'available'::drip_job_state
                      AND queue = ?
                      AND scheduled_at <= ?
                    ORDER BY priority ASC, scheduled_at ASC, id ASC
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED"
                 queue (encode-ts now) (int limit)]
                db/jdbc-opts)]
      (when (seq rows)
        (let [ids (mapv :id rows)
              [in-sql in-params] (in-clause ids)]
          (mapv row->job
                (jdbc/execute!
                 tx
                 (into [(str "UPDATE drip_job
                                SET state = 'running'::drip_job_state,
                                    attempt = attempt + 1,
                                    attempted_at = ?,
                                    attempted_by = array_append(attempted_by, ?)
                                WHERE id IN " in-sql "
                                RETURNING *")
                        (encode-ts now)
                        worker-id]
                       in-params)
                 db/jdbc-opts))))))

  (record-output! [_ tx job-id output]
    (row->job
     (jdbc/execute-one!
      tx
      ["UPDATE drip_job SET metadata = metadata || ?::jsonb WHERE id = ? RETURNING *"
       (db/->json-str {:output output}) job-id]
      db/jdbc-opts)))

  (complete-job! [_ tx job-id]
    (let [now (encode-ts (Instant/now))
          result (jdbc/execute-one!
                  tx
                  ["UPDATE drip_job SET state = 'completed'::drip_job_state, finalized_at = ?
                     WHERE id = ? AND state = 'running'::drip_job_state
                     RETURNING *"
                   now job-id]
                  db/jdbc-opts)
          job (row->job (or result (jdbc/execute-one! tx ["SELECT * FROM drip_job WHERE id = ?" job-id] db/jdbc-opts)))]
      (when (:ephemeral job)
        (jdbc/execute-one! tx ["DELETE FROM drip_job WHERE id = ?" job-id]))
      job))

  (fail-job! [_ tx job-id error-map retry-policy]
    (let [now (Instant/now)
          row (jdbc/execute-one! tx
                                 ["SELECT attempt, max_attempts FROM drip_job WHERE id = ?" job-id]
                                 db/jdbc-opts)
          error-entry (assoc error-map :at (str now))
          exhausted? (>= (:attempt row) (:max-attempts row))
          new-state (if exhausted? "discarded" "retryable")
          next-run (when-not exhausted? (retry-policy (:attempt row)))
          result (jdbc/execute-one!
                  tx
                  ["UPDATE drip_job
                     SET state = ?::drip_job_state,
                         errors = array_append(errors, ?::jsonb),
                         scheduled_at = COALESCE(?, scheduled_at),
                         finalized_at = ?
                     WHERE id = ? AND state = 'running'::drip_job_state
                     RETURNING *"
                   new-state
                   (db/->json-str error-entry)
                   (encode-ts next-run)
                   (encode-ts (when exhausted? now))
                   job-id]
                  db/jdbc-opts)]
      (row->job (or result (jdbc/execute-one! tx
                                              ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                              db/jdbc-opts)))))

  (cancel-job! [_ tx job-id]
    (let [now (encode-ts (Instant/now))
          result (jdbc/execute-one!
                  tx
                  ["UPDATE drip_job
                     SET state = 'cancelled'::drip_job_state,
                         finalized_at = ?
                     WHERE id = ?
                       AND state NOT IN ('cancelled'::drip_job_state,'completed'::drip_job_state,'discarded'::drip_job_state)
                     RETURNING *"
                   now job-id]
                  db/jdbc-opts)]
      (row->job (or result (jdbc/execute-one! tx
                                              ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                              db/jdbc-opts)))))

  (retry-job! [_ tx job-id]
    (let [now (encode-ts (Instant/now))
          result (jdbc/execute-one!
                  tx
                  ["UPDATE drip_job SET state = 'available'::drip_job_state, scheduled_at = ?, finalized_at = NULL
                     WHERE id = ?
                       AND state IN ('retryable'::drip_job_state,'discarded'::drip_job_state,'cancelled'::drip_job_state)
                     RETURNING *"
                   now job-id]
                  db/jdbc-opts)]
      (row->job (or result (jdbc/execute-one! tx
                                              ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                              db/jdbc-opts)))))

  (discard-job! [_ tx job-id]
    (let [now (encode-ts (Instant/now))
          result (jdbc/execute-one!
                  tx
                  ["UPDATE drip_job
                     SET state = 'discarded'::drip_job_state,
                         finalized_at = ?
                     WHERE id = ?
                       AND state NOT IN ('completed'::drip_job_state,'discarded'::drip_job_state)
                     RETURNING *"
                   now job-id]
                  db/jdbc-opts)]
      (row->job (or result (jdbc/execute-one! tx
                                              ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                              db/jdbc-opts)))))

  (snooze-job! [_ tx job-id dur]
    (let [wake-at (encode-ts (.plusMillis (Instant/now) (long (duration/duration dur))))
          result (jdbc/execute-one!
                  tx
                  ["UPDATE drip_job
                     SET state = 'scheduled'::drip_job_state,
                         scheduled_at = ?,
                         finalized_at = NULL
                     WHERE id = ? AND state = 'running'::drip_job_state
                     RETURNING *"
                   wake-at job-id]
                  db/jdbc-opts)]
      (row->job (or result (jdbc/execute-one! tx ["SELECT * FROM drip_job WHERE id = ?" job-id] db/jdbc-opts)))))

  (update-job! [_ tx job-id opts]
    (let [now (Instant/now)
          {:keys [metadata priority queue scheduled-at state max-attempts tags ephemeral]} opts
          sets (cond-> []
                 (contains? opts :metadata) (conj ["metadata = ?::jsonb" (db/->json-str metadata)])
                 (contains? opts :priority) (conj ["priority = ?" (int priority)])
                 (contains? opts :queue) (conj ["queue = ?" queue])
                 (contains? opts :max-attempts) (conj ["max_attempts = ?" (int max-attempts)])
                 (contains? opts :ephemeral) (conj ["ephemeral = ?" (boolean ephemeral)])
                 (contains? opts :tags) (conj ["tags = ?"
                                               ^"[Ljava.lang.String;" (into-array String (map str tags))])
                 (contains? opts :scheduled-at) (conj ["scheduled_at = ?" (encode-ts scheduled-at)]
                                                      ["state = ?::drip_job_state"
                                                       (if (and scheduled-at (.isAfter ^Instant scheduled-at now))
                                                         "scheduled"
                                                         "available")])
                 (and (contains? opts :state) (not (contains? opts :scheduled-at)))
                 (conj ["state = ?::drip_job_state" (name state)]))]
      (when (seq sets)
        (let [sql (str "UPDATE drip_job SET "
                       (str/join ", " (map first sets))
                       " WHERE id = ?")
              params (conj (mapv second sets) job-id)]
          (jdbc/execute-one! tx (into [sql] params))))
      (row->job (jdbc/execute-one! tx
                                   ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                   db/jdbc-opts))))

  (delete-job! [_ tx job-id]
    (let [job (row->job (jdbc/execute-one! tx
                                           ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                           db/jdbc-opts))]
      (when job
        (jdbc/execute-one! tx ["DELETE FROM drip_job WHERE id = ?" job-id]))
      job))

  (promote-scheduled-jobs! [_ tx]
    (let [result (jdbc/execute-one!
                  tx
                  ["UPDATE drip_job SET state = 'available'::drip_job_state
                     WHERE state IN ('scheduled'::drip_job_state,'retryable'::drip_job_state)
                       AND scheduled_at <= ?"
                   (encode-ts (Instant/now))])]
      (:next.jdbc/update-count result 0)))

  (rescue-stuck-jobs! [_ tx stuck-after retry-policy queues]
    (let [now (Instant/now)
          queue-names (when (seq queues) (vec queues))
          sql (cond-> "SELECT id, attempt, max_attempts FROM drip_job
                        WHERE state = 'running'::drip_job_state
                          AND attempted_at <= ?"
                (seq queue-names) (str " AND queue IN ("
                                       (str/join "," (repeat (count queue-names) "?"))
                                       ")"))
          params (cond-> [(encode-ts stuck-after)]
                   (seq queue-names) (into queue-names))
          stuck (jdbc/execute! tx (into [sql] params) db/jdbc-opts)
          error-entry {:error "job rescued after timeout" :at (str now)}]
      (reduce
       (fn [^long n row]
         (let [exhausted? (>= (:attempt row) (:max-attempts row))
               new-state (if exhausted? "discarded" "retryable")
               next-run (when-not exhausted? (retry-policy (:attempt row)))]
           (jdbc/execute-one!
            tx
            ["UPDATE drip_job
               SET state = ?::drip_job_state,
                   errors = array_append(errors, ?::jsonb),
                   scheduled_at = COALESCE(?, scheduled_at),
                   finalized_at = ?
               WHERE id = ? AND state = 'running'::drip_job_state"
             new-state
             (db/->json-str error-entry)
             (encode-ts next-run)
             (encode-ts (when exhausted? now))
             (:id row)])
           (inc n)))
       0
       stuck)))

  (delete-jobs! [_ tx {:keys [states kinds queues priorities finalized-before created-before]}]
    (let [state-names (mapv name states)
          state-placeholders (str/join "," (repeat (count state-names) "?::drip_job_state"))
          kind-names (when (seq kinds) (vec kinds))
          queue-names (when (seq queues) (vec queues))
          priority-ints (when (seq priorities) (mapv int priorities))
          conditions (cond-> [(str "state IN (" state-placeholders ")")]
                       (seq kind-names) (conj (str "kind IN (" (str/join "," (repeat (count kind-names) "?")) ")"))
                       (seq queue-names) (conj (str "queue IN (" (str/join "," (repeat (count queue-names) "?")) ")"))
                       (seq priority-ints) (conj (str "priority IN (" (str/join "," (repeat (count priority-ints) "?")) ")"))
                       finalized-before (conj "finalized_at <= ?")
                       created-before (conj "created_at <= ?"))
          params (cond-> state-names
                   (seq kind-names) (into kind-names)
                   (seq queue-names) (into queue-names)
                   (seq priority-ints) (into priority-ints)
                   finalized-before (conj (encode-ts finalized-before))
                   created-before (conj (encode-ts created-before)))
          sql (str "DELETE FROM drip_job WHERE " (str/join " AND " conditions))
          result (jdbc/execute-one! tx (into [sql] params))]
      (:next.jdbc/update-count result 0)))

  (get-job! [_ tx job-id]
    (row->job (jdbc/execute-one! tx ["SELECT * FROM drip_job WHERE id = ?" job-id] db/jdbc-opts)))

  (list-jobs! [_ tx {:keys [state states kind kinds queue queues priorities
                            created-after created-before scheduled-after scheduled-before
                            limit after]
                     :or {limit 100}}]
    (when (and state (not (job/states state)))
      (throw (IllegalArgumentException. (str "Unknown state: " state))))
    (doseq [s states]
      (when-not (job/states s)
        (throw (IllegalArgumentException. (str "Unknown state: " s)))))
    (let [all-states (cond-> []
                       state (conj (name state))
                       (seq states) (into (mapv name states)))
          all-kinds (cond-> []
                      kind (conj kind)
                      (seq kinds) (into (vec kinds)))
          all-queues (cond-> []
                       queue (conj queue)
                       (seq queues) (into (vec queues)))
          priority-ints (when (seq priorities) (mapv int priorities))
          conditions (cond-> ["1=1"]
                       (seq all-states) (conj (str "state IN ("
                                                   (str/join "," (repeat (count all-states) "?::drip_job_state"))
                                                   ")"))
                       (seq all-kinds) (conj (str "kind IN (" (str/join "," (repeat (count all-kinds) "?")) ")"))
                       (seq all-queues) (conj (str "queue IN (" (str/join "," (repeat (count all-queues) "?")) ")"))
                       (seq priority-ints) (conj (str "priority IN (" (str/join "," (repeat (count priority-ints) "?")) ")"))
                       created-after (conj "created_at >= ?")
                       created-before (conj "created_at <= ?")
                       scheduled-after (conj "scheduled_at >= ?")
                       scheduled-before (conj "scheduled_at <= ?")
                       after (conj "id < ?"))
          params (cond-> []
                   (seq all-states) (into all-states)
                   (seq all-kinds) (into all-kinds)
                   (seq all-queues) (into all-queues)
                   (seq priority-ints) (into priority-ints)
                   created-after (conj (encode-ts created-after))
                   created-before (conj (encode-ts created-before))
                   scheduled-after (conj (encode-ts scheduled-after))
                   scheduled-before (conj (encode-ts scheduled-before))
                   after (conj after))
          sql (str "SELECT * FROM drip_job WHERE "
                   (str/join " AND " conditions)
                   " ORDER BY id DESC LIMIT " (int limit))]
      (mapv row->job (jdbc/execute! tx (into [sql] params) db/jdbc-opts))))

  client/Queues
  (upsert-queue! [_ tx queue-name metadata]
    (jdbc/execute-one!
     tx
     ["INSERT INTO drip_queue (name, metadata, updated_at)
         VALUES (?, ?::jsonb, ?)
         ON CONFLICT (name) DO UPDATE SET
           metadata   = EXCLUDED.metadata,
           updated_at = EXCLUDED.updated_at"
      queue-name (db/->json-str metadata) (encode-ts (Instant/now))]))

  (pause-queue! [_ tx queue-name]
    (let [now (encode-ts (Instant/now))]
      (jdbc/execute-one!
       tx
       ["UPDATE drip_queue SET paused_at = ?, updated_at = ? WHERE name = ?"
        now now queue-name])))

  (resume-queue! [_ tx queue-name]
    (jdbc/execute-one!
     tx
     ["UPDATE drip_queue SET paused_at = NULL, updated_at = ? WHERE name = ?"
      (encode-ts (Instant/now)) queue-name]))

  (queue-paused? [_ tx queue-name]
    (let [row (jdbc/execute-one!
               tx
               ["SELECT paused_at FROM drip_queue WHERE name = ?" queue-name]
               db/jdbc-opts)]
      (boolean (and row (:paused-at row)))))

  (list-queues! [_ tx]
    (jdbc/execute! tx ["SELECT * FROM drip_queue ORDER BY name"] db/jdbc-opts))

  client/FastBulkInsert
  (insert-many-fast! [{:keys [ds]} job-specs]
    (let [now (Instant/now)
          sb (StringBuilder.)
          _ (doseq [[kind args opts] job-specs] (append-copy-row sb kind args opts now))
          data (.getBytes (.toString sb) ^java.nio.charset.Charset StandardCharsets/UTF_8)
          in (ByteArrayInputStream. data)]
      (with-open [conn (.getConnection ^javax.sql.DataSource ds)]
        (long (.copyIn ^CopyManager (.getCopyAPI (pg-conn conn))
                       "COPY drip_job (state, attempt, max_attempts, scheduled_at, priority, args, attempted_by, errors, kind, metadata, queue, tags) FROM STDIN"
                       in)))))

  client/Maintenance
  (reindex! [{:keys [ds]}]
    (let [index-names ["drip_job_prioritized_fetching_index"
                       "drip_job_kind"
                       "drip_job_state_and_finalized_at_index"
                       "drip_job_args_index"
                       "drip_job_metadata_index"
                       "drip_job_unique_idx"]]
      (with-open [conn (.getConnection ^javax.sql.DataSource ds)]
        (reduce
         (fn [results index-name]
           (let [exists? (-> (jdbc/execute-one!
                              conn
                              ["SELECT 1 FROM pg_class WHERE relname = ? AND relkind = 'i'" index-name]
                              db/jdbc-opts)
                             some?)
                 artifact? (-> (jdbc/execute-one!
                                conn
                                ["SELECT 1 FROM pg_class WHERE relname IN (?, ?) AND relkind = 'i'"
                                 (str index-name "_ccnew") (str index-name "_ccold")]
                                db/jdbc-opts)
                               some?)]
             (assoc results
                    (keyword index-name)
                    (cond
                      (not exists?) :not-found
                      artifact? :skipped
                      :else (do
                              (jdbc/execute! conn [(str "REINDEX INDEX CONCURRENTLY " index-name)])
                              :reindexed)))))
         {}
         index-names)))))

(defn make-client
  "Returns a PostgreSQL client. `ds` is a javax.sql.DataSource."
  [ds]
  (->PostgresClient ds))
