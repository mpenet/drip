(ns s-exp.drip.client.sqlite
  "SQLite client.
   Key differences from MariaDB/PostgreSQL:
     - No FOR UPDATE SKIP LOCKED; SQLite serializes writes at the DB level
     - Timestamps stored as ISO-8601 TEXT
     - JSON stored as TEXT; json_insert/json_array require SQLite 3.38.0+
     - INTEGER PRIMARY KEY AUTOINCREMENT; generated key is :last_insert_rowid()
     - No NOTIFY/LISTEN"
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [s-exp.drip.client :as client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.duration :as duration])
  (:import (java.time Instant)
           (org.sqlite SQLiteErrorCode SQLiteException)))

(set! *warn-on-reflection* true)

(defn- in-clause [ids]
  [(str "(" (str/join "," (repeat (count ids) "?")) ")")
   ids])

(defn- encode-ts ^String [^Instant i]
  (db/instant->str i))

(defn- row->job [row]
  (when row
    {:id (:id row)
     :attempt (:attempt row)
     :attempted-at (db/ts->instant (:attempted-at row))
     :attempted-by (db/<-json (:attempted-by row))
     :created-at (db/ts->instant (:created-at row))
     :args (db/<-json (:encoded-args row))
     :errors (db/<-json (:errors row))
     :finalized-at (db/ts->instant (:finalized-at row))
     :kind (:kind row)
     :max-attempts (:max-attempts row)
     :metadata (db/<-json-metadata (:metadata row))
     :priority (:priority row)
     :queue (:queue row)
     :scheduled-at (db/ts->instant (:scheduled-at row))
     :state (keyword (:state row))
     :tags (db/<-json (:tags row))
     :unique-key (:unique-key row)
     :unique-states (:unique-states row)
     :ephemeral (= 1 (:ephemeral row))}))

(defrecord SQLiteClient [ds]
  client/Migration
  (migration-config [_]
    {:migration-dir "migrations/sqlite"
     :migration-table-name "drip_migration"})

  client/Notifications
  (notify-job-available! [_ _queue] nil)
  (start-listener! [_ _on-notify] nil)
  (stop-listener! [_ _listener] nil)

  client/Jobs
  (insert-job! [_ tx kind args opts]
    (let [opts (merge job/default-insert-opts opts)
          now (Instant/now)
          queue (:queue opts)
          encoded-args (db/->json args)
          unique-opts (:unique-opts opts)
          unique-key (db/compute-unique-key kind args queue now unique-opts)
          unique-states (when unique-opts
                          (job/states->bitmask
                           (or (:by-state unique-opts) job/default-unique-states)))
          scheduled-at (or (:scheduled-at opts) now)
          initial-state (if (.isAfter ^Instant scheduled-at now) "scheduled" "available")
          result (try
                   (jdbc/execute-one!
                    tx
                    ["INSERT INTO drip_job
                        (attempt, attempted_by, encoded_args, errors,
                         kind, max_attempts, metadata, priority,
                         queue, scheduled_at, state, tags,
                         unique_key, unique_states, ephemeral)
                      VALUES (0, json_array(), ?, json_array(),
                              ?, ?, ?, ?,
                              ?, ?, ?, ?,
                              ?, ?, ?)"
                     encoded-args
                     kind
                     (int (:max-attempts opts))
                     (db/->json-str (:metadata opts))
                     (int (:priority opts))
                     queue
                     (encode-ts scheduled-at)
                     initial-state
                     (db/->json-str (:tags opts))
                     unique-key
                     unique-states
                     (if (:ephemeral opts) 1 0)]
                    {:return-keys true})
                   (catch SQLiteException e
                     (when-not (= SQLiteErrorCode/SQLITE_CONSTRAINT_UNIQUE (.getResultCode ^SQLiteException e))
                       (throw e))
                     (throw (db/unique-conflict-ex kind queue unique-opts e))))]
      (row->job
       (jdbc/execute-one!
        tx
        ["SELECT * FROM drip_job WHERE id = ?"
         (get result (keyword "last_insert_rowid()"))]
        db/jdbc-opts))))

  (fetch-jobs! [_ tx queue worker-id opts]
    (let [{:keys [limit] :or {limit 10}} opts
          now (Instant/now)
          rows (jdbc/execute!
                tx
                ["SELECT * FROM drip_job
                    WHERE queue = ?
                      AND state = 'available'
                      AND scheduled_at <= ?
                    ORDER BY priority ASC, scheduled_at ASC
                    LIMIT ?"
                 queue (encode-ts now) (int limit)]
                db/jdbc-opts)]
      (when (seq rows)
        (let [ids (mapv :id rows)
              [in-sql in-params] (in-clause ids)]
          (jdbc/execute!
           tx
           (into [(str "UPDATE drip_job
                          SET state = 'running',
                              attempt = attempt + 1,
                              attempted_at = ?,
                              attempted_by = json_insert(
                                COALESCE(attempted_by, json_array()), '$[#]', ?)
                              WHERE id IN " in-sql)
                  (encode-ts now)
                  worker-id]
                 in-params))
          (mapv row->job
                (jdbc/execute!
                 tx
                 (into [(str "SELECT * FROM drip_job WHERE id IN " in-sql)]
                       in-params)
                 db/jdbc-opts))))))

  (record-output! [_ tx job-id output]
    (jdbc/execute-one!
     tx
     ["UPDATE drip_job SET metadata = json_patch(metadata, ?) WHERE id = ?"
      (db/->json-str {:output output}) job-id])
    (row->job (jdbc/execute-one! tx
                                 ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                 db/jdbc-opts)))

  (complete-job! [_ tx job-id]
    (let [now (encode-ts (Instant/now))]
      (jdbc/execute-one!
       tx
       ["UPDATE drip_job
          SET state = 'completed',
              finalized_at = ?,
              unique_key = CASE WHEN unique_states IS NOT NULL AND (unique_states & 32) = 0 THEN NULL ELSE unique_key END
          WHERE id = ? AND state = 'running'"
        now job-id])
      (let [job (row->job (jdbc/execute-one! tx
                                             ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                             db/jdbc-opts))]
        (when (:ephemeral job)
          (jdbc/execute-one! tx ["DELETE FROM drip_job WHERE id = ?" job-id]))
        job)))

  (fail-job! [_ tx job-id error-map retry-policy]
    (let [now (Instant/now)
          job (row->job (jdbc/execute-one! tx
                                           ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                           db/jdbc-opts))
          error-entry (assoc error-map :at (str now))
          new-errors (conj (or (:errors job) []) error-entry)
          exhausted? (>= (:attempt job) (:max-attempts job))
          new-state (if exhausted? "discarded" "retryable")
          new-state-bit (if exhausted? 16 4)
          next-run (when-not exhausted? (retry-policy (:attempt job)))]
      (jdbc/execute-one!
       tx
       ["UPDATE drip_job
          SET state = ?,
              errors = ?,
              scheduled_at = COALESCE(?, scheduled_at),
              finalized_at = ?,
              unique_key = CASE WHEN unique_states IS NOT NULL AND (unique_states & ?) = 0 THEN NULL ELSE unique_key END
          WHERE id = ? AND state = 'running'"
        new-state
        (db/->json-str new-errors)
        (encode-ts next-run)
        (encode-ts (when exhausted? now))
        new-state-bit
        job-id])
      (row->job (jdbc/execute-one! tx
                                   ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                   db/jdbc-opts))))

  (cancel-job! [_ tx job-id]
    (let [now (encode-ts (Instant/now))]
      (jdbc/execute-one!
       tx
       ["UPDATE drip_job
          SET state = 'cancelled',
              finalized_at = ?,
              unique_key = CASE WHEN unique_states IS NOT NULL AND (unique_states & 64) = 0 THEN NULL ELSE unique_key END
          WHERE id = ? AND state NOT IN ('cancelled','completed','discarded')"
        now job-id])
      (row->job (jdbc/execute-one! tx
                                   ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                   db/jdbc-opts))))

  (retry-job! [_ tx job-id]
    (let [now (encode-ts (Instant/now))]
      (jdbc/execute-one!
       tx
       ["UPDATE drip_job SET state = 'available', scheduled_at = ?, finalized_at = NULL
          WHERE id = ? AND state IN ('retryable','discarded','cancelled')"
        now job-id])
      (row->job (jdbc/execute-one! tx
                                   ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                   db/jdbc-opts))))

  (discard-job! [_ tx job-id]
    (let [now (encode-ts (Instant/now))]
      (jdbc/execute-one!
       tx
       ["UPDATE drip_job
          SET state = 'discarded',
              finalized_at = ?,
              unique_key = CASE WHEN unique_states IS NOT NULL AND (unique_states & 16) = 0 THEN NULL ELSE unique_key END
          WHERE id = ? AND state NOT IN ('completed','discarded')"
        now job-id])
      (row->job (jdbc/execute-one! tx
                                   ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                   db/jdbc-opts))))

  (snooze-job! [_ tx job-id dur]
    (let [wake-at (encode-ts (.plusMillis (Instant/now) (long (duration/duration dur))))]
      (jdbc/execute-one!
       tx
       ["UPDATE drip_job
          SET state = 'scheduled',
              scheduled_at = ?,
              finalized_at = NULL
          WHERE id = ? AND state = 'running'"
        wake-at job-id])
      (row->job (jdbc/execute-one! tx
                                   ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                   db/jdbc-opts))))

  (update-job! [_ tx job-id opts]
    (let [now (Instant/now)
          {:keys [metadata priority queue scheduled-at state max-attempts tags ephemeral]} opts
          sets (cond-> []
                 (contains? opts :metadata) (conj ["metadata = ?" (db/->json-str metadata)])
                 (contains? opts :priority) (conj ["priority = ?" (int priority)])
                 (contains? opts :queue) (conj ["queue = ?" queue])
                 (contains? opts :max-attempts) (conj ["max_attempts = ?" (int max-attempts)])
                 (contains? opts :ephemeral) (conj ["ephemeral = ?" (if ephemeral 1 0)])
                 (contains? opts :tags) (conj ["tags = ?" (db/->json-str tags)])
                 (contains? opts :scheduled-at) (conj ["scheduled_at = ?" (encode-ts scheduled-at)]
                                                      ["state = ?"
                                                       (if (and scheduled-at (.isAfter ^Instant scheduled-at now))
                                                         "scheduled"
                                                         "available")])
                 (and (contains? opts :state) (not (contains? opts :scheduled-at)))
                 (conj ["state = ?" (name state)]))]
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
                  ["UPDATE drip_job SET state = 'available'
                     WHERE state IN ('scheduled','retryable')
                       AND scheduled_at <= ?"
                   (encode-ts (Instant/now))])]
      (:next.jdbc/update-count result 0)))

  (rescue-stuck-jobs! [_ tx stuck-after retry-policy queues]
    (let [now (Instant/now)
          queue-names (when (seq queues) (vec queues))
          sql (cond-> "SELECT id, attempt, max_attempts FROM drip_job
                        WHERE state = 'running'
                          AND attempted_at <= ?"
                (seq queue-names) (str " AND queue IN " (first (in-clause queue-names))))
          params (cond-> [(encode-ts stuck-after)]
                   (seq queue-names) (into queue-names))
          stuck (jdbc/execute! tx (into [sql] params) db/jdbc-opts)
          error-entry {:error "job rescued after timeout" :at (str now)}]
      (reduce
       (fn [^long n row]
         (let [exhausted? (>= (:attempt row) (:max-attempts row))
               new-state (if exhausted? "discarded" "retryable")
               new-state-bit (if exhausted? 16 4)
               next-run (when-not exhausted? (retry-policy (:attempt row)))
               job (row->job (jdbc/execute-one! tx
                                                ["SELECT * FROM drip_job WHERE id = ?" (:id row)]
                                                db/jdbc-opts))
               new-errors (conj (or (:errors job) []) error-entry)]
           (jdbc/execute-one!
            tx
            ["UPDATE drip_job
               SET state = ?,
                   errors = ?,
                   scheduled_at = COALESCE(?, scheduled_at),
                   finalized_at = ?,
                   unique_key = CASE WHEN unique_states IS NOT NULL AND (unique_states & ?) = 0 THEN NULL ELSE unique_key END
               WHERE id = ? AND state = 'running'"
             new-state
             (db/->json-str new-errors)
             (encode-ts next-run)
             (encode-ts (when exhausted? now))
             new-state-bit
             (:id row)])
           (inc n)))
       0
       stuck)))

  (delete-jobs! [_ tx {:keys [states kinds queues priorities finalized-before created-before]}]
    (let [state-names (mapv name states)
          [state-in-sql _] (in-clause state-names)
          kind-names (when (seq kinds) (vec kinds))
          queue-names (when (seq queues) (vec queues))
          priority-ints (when (seq priorities) (mapv int priorities))
          conditions (cond-> [(str "state IN " state-in-sql)]
                       (seq kind-names) (conj (str "kind IN " (first (in-clause kind-names))))
                       (seq queue-names) (conj (str "queue IN " (first (in-clause queue-names))))
                       (seq priority-ints) (conj (str "priority IN " (first (in-clause priority-ints))))
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
    (row->job (jdbc/execute-one! tx
                                 ["SELECT * FROM drip_job WHERE id = ?" job-id]
                                 db/jdbc-opts)))

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
                       (seq all-states) (conj (str "state IN " (first (in-clause all-states))))
                       (seq all-kinds) (conj (str "kind IN " (first (in-clause all-kinds))))
                       (seq all-queues) (conj (str "queue IN " (first (in-clause all-queues))))
                       (seq priority-ints) (conj (str "priority IN " (first (in-clause priority-ints))))
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
         VALUES (?, ?, ?)
         ON CONFLICT (name) DO UPDATE SET
           metadata   = excluded.metadata,
           updated_at = excluded.updated_at"
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
    (if (empty? job-specs)
      0
      (let [now (Instant/now)
            placeholder "(0, '[]', ?, '[]', ?, ?, ?, ?, ?, ?, ?, '[]', 0)"
            sql (str "INSERT INTO drip_job "
                     "(attempt, attempted_by, encoded_args, errors, "
                     "kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, ephemeral) "
                     "VALUES "
                     (str/join "," (repeat (count job-specs) placeholder)))
            params (into []
                         (mapcat (fn [[kind args opts]]
                                   (let [opts (merge job/default-insert-opts opts)
                                         scheduled-at (or (:scheduled-at opts) now)
                                         state (if (.isAfter ^Instant scheduled-at now) "scheduled" "available")]
                                     [(db/->json-str args)
                                      kind
                                      (int (:max-attempts opts))
                                      (db/->json-str (:metadata opts))
                                      (int (:priority opts))
                                      (:queue opts)
                                      (encode-ts scheduled-at)
                                      state]))
                                 job-specs))]
        (with-open [conn (.getConnection ^javax.sql.DataSource ds)]
          (long (:next.jdbc/update-count
                 (jdbc/execute-one! conn (into [sql] params) {})))))))

  client/Maintenance
  (reindex! [_] {}))

(defn make-client
  "Returns a SQLite client. `ds` is a javax.sql.DataSource.
   Enables WAL mode and sets a 5-second busy timeout so concurrent
   worker threads can share the same database file without SQLITE_BUSY errors."
  [ds]
  (Class/forName "org.sqlite.JDBC")
  (jdbc/execute! ds ["PRAGMA journal_mode=WAL"])
  (jdbc/execute! ds ["PRAGMA busy_timeout=5000"])
  (->SQLiteClient ds))
