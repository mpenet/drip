(ns s-exp.drip.db
  (:require [clojure.string :as str]
            [jsonista.core :as json]
            [migratus.core :as migratus]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [s-exp.drip.client :as client]
            [s-exp.drip.job :as job]
            [s-exp.duration :as duration])
  (:import (com.fasterxml.jackson.databind MapperFeature ObjectMapper SerializationFeature)
           (java.math BigInteger)
           (java.security MessageDigest)
           (java.sql Blob Timestamp)
           (java.time Instant)
           (java.time.format DateTimeFormatter)))

;; ---------------------------------------------------------------------------
;; JSON codec
;; ---------------------------------------------------------------------------

(def ^ObjectMapper mapper
  (json/object-mapper
   {:encode-key-fn name
    :decode-key-fn keyword
    :mapper-fn (fn [^ObjectMapper m]
                 (.enable m ^"[Lcom.fasterxml.jackson.databind.MapperFeature;"
                          (into-array MapperFeature [MapperFeature/SORT_PROPERTIES_ALPHABETICALLY]))
                 (.disable m SerializationFeature/FAIL_ON_EMPTY_BEANS)
                 m)}))

;; Metadata is user-controlled and may contain non-keyword-safe keys (spaces,
;; numbers, etc.).  Decode it with string keys to match River's behaviour and
;; avoid lossy round-trips.
(def ^:private ^ObjectMapper str-mapper
  (json/object-mapper
   {:encode-key-fn name
    :mapper-fn (fn [^ObjectMapper m]
                 (.disable m SerializationFeature/FAIL_ON_EMPTY_BEANS)
                 m)}))

(defn ->json ^bytes [v]
  (json/write-value-as-bytes v mapper))

(defn ->json-str ^String [v]
  (json/write-value-as-string v mapper))

(defn- read-json-value [^ObjectMapper m v]
  (cond
    (bytes? v) (json/read-value ^bytes v m)
    (string? v) (json/read-value ^String v m)
    (instance? Blob v) (json/read-value (.getBinaryStream ^Blob v) m)
    ;; PostgreSQL jsonb columns arrive as PGobject; getValue() returns JSON string.
    (= "org.postgresql.util.PGobject" (.getName (class v)))
    (json/read-value ^String (let [meth (.getMethod (class v) "getValue" (into-array Class []))]
                               (.invoke meth v (into-array Object []))) m)
    :else (throw (IllegalArgumentException. (str "Cannot decode JSON from: " (class v))))))

(defn <-json [v]
  (when v (read-json-value mapper v)))

(defn <-json-metadata [v]
  "Decodes a JSON metadata column with string keys (not keywords)."
  (when v (read-json-value str-mapper v)))

;; ---------------------------------------------------------------------------
;; Timestamp handling - always UTC
;;
;; MariaDB/PostgreSQL JDBC drivers return java.sql.Timestamp.
;; SQLite JDBC driver returns TEXT columns as String (ISO-8601).
;; ts->instant handles both; callers use instant->ts for MariaDB/PG params
;; and instant->str for SQLite params (passed as ISO-8601 strings).
;; ---------------------------------------------------------------------------

(def ^:private ^DateTimeFormatter iso-instant-fmt
  DateTimeFormatter/ISO_INSTANT)

(defn ts->instant ^Instant [v]
  (cond
    (nil? v) nil
    (instance? Timestamp v) (.toInstant ^Timestamp v)
    (instance? Long v) (Instant/ofEpochMilli ^Long v)
    (string? v) (try (Instant/parse ^String v)
                     (catch Exception _
                       (Instant/ofEpochMilli (Long/parseLong ^String v))))
    :else (throw (IllegalArgumentException. (str "Cannot convert to Instant: " (class v))))))

(defn instant->ts ^Timestamp [^Instant i]
  (when i (Timestamp/from i)))

(defn instant->str ^String [^Instant i]
  (when i (.format iso-instant-fmt i)))

;; ---------------------------------------------------------------------------
;; next.jdbc options and transaction helper
;; ---------------------------------------------------------------------------

(def jdbc-opts
  {:builder-fn rs/as-unqualified-kebab-maps})

(defmacro with-tx
  "Opens a transaction from client and binds it to tx-sym, passing remaining
   args to next.jdbc/with-transaction. Body executes within the transaction.

   Example:
     (db/with-tx [tx client]
       (insert-job! client tx \"k\" {} nil)
       (my-business-write! tx data))"
  [[tx-sym client & jdbc-opts] & body]
  `(jdbc/with-transaction [~tx-sym (:ds ~client) ~@jdbc-opts]
     ~@body))

(defmacro with-conn
  "Opens an autocommit connection from client, binds it to conn-sym and closes
   it after body. For reads that need no transaction: unlike with-tx it never
   takes a write lock up front (SQLite IMMEDIATE mode), and it keeps the
   connection open while rows are converted (PostgreSQL arrays are read lazily)."
  [[conn-sym client] & body]
  `(with-open [~conn-sym (jdbc/get-connection (:ds ~client))]
     ~@body))

;; ---------------------------------------------------------------------------
;; Unique key computation (shared across standard dialects)
;; ---------------------------------------------------------------------------

(defn- sha256 ^bytes [^String s]
  (let [^MessageDigest md (MessageDigest/getInstance "SHA-256")]
    (.digest md (.getBytes s "UTF-8"))))

(defn- period-floor-ms ^long [^long epoch-ms ^long period-ms]
  (* (quot epoch-ms period-ms) period-ms))

(defn- canonicalize
  "Returns v with every map, at any depth, turned into a sorted map with
   string keys (encoded as the JSON mapper encodes them), so equal args always
   serialize to identical bytes."
  [v]
  (cond
    (map? v) (into (sorted-map)
                   (map (fn [[k x]]
                          [(if (instance? clojure.lang.Named k) (name k) (str k))
                           (canonicalize x)]))
                   v)
    (sequential? v) (mapv canonicalize v)
    :else v))

(defn- args->hex ^String [args]
  (.toString (BigInteger. 1 (->json (canonicalize args))) 16))

(defn compute-unique-key
  "Computes a 32-byte SHA-256 unique key for a job.
   Returns nil when unique-opts is nil (no uniqueness constraint).
   args is the raw (pre-serialization) args map."
  ^bytes [kind args queue ^Instant now unique-opts]
  (when unique-opts
    (let [{:keys [by-args by-keys by-period by-queue exclude-kind]} unique-opts
          sb (StringBuilder.)]
      (when-not exclude-kind
        (.append sb "kind=")
        (.append sb ^String kind))
      (cond
        (seq by-keys)
        (do (.append sb "&args=")
            (.append sb (args->hex (select-keys args by-keys))))
        by-args
        (do (.append sb "&args=")
            (.append sb (args->hex args))))
      (when by-period
        (let [floor-ms (period-floor-ms (.toEpochMilli now) (long (duration/duration by-period)))]
          (.append sb "&period=")
          (.append sb floor-ms)))
      (when by-queue
        (.append sb "&queue=")
        (.append sb ^String queue))
      (sha256 (.toString sb)))))

;; ---------------------------------------------------------------------------
;; Unique conflict error
;; ---------------------------------------------------------------------------

(defn unique-conflict-ex
  "Builds an ExceptionInfo for a unique job constraint violation.
   data is merged into the ex-data; cause is the originating SQLException."
  [data ^Throwable cause]
  (ex-info "Unique job conflict"
           (assoc data :type :s-exp.drip/unique-conflict)
           cause))

(defn claim-lost-ex
  "Builds the ExceptionInfo thrown when job is no longer running under the
   claim it was fetched with (it finished, was rescued, or was re-claimed)."
  [job]
  (ex-info "Job claim lost"
           {:type :s-exp.drip/claim-lost
            :job-id (:id job)
            :claim (job/claim job)}))

(defn insert-unique!
  "Inserts a job via (insert!), which returns the new Job. When the insert
   hits a unique violation, returns the job holding the slot, as returned by
   (find-holder), instead. Results carry :unique-skipped. If the holder left
   its covered states in the meantime, the insert is retried.

   Only for MariaDB and SQLite, where a failed statement does not abort the
   surrounding transaction."
  [unique-violation? insert! find-holder conflict-data]
  (loop [attempts 3]
    (let [result (try
                   (insert!)
                   (catch java.sql.SQLException e
                     (if (unique-violation? e) e (throw e))))]
      (if-not (instance? java.sql.SQLException result)
        (assoc result :unique-skipped false)
        (if-let [holder (find-holder)]
          (assoc holder :unique-skipped true)
          (if (> attempts 1)
            (recur (dec attempts))
            (throw (unique-conflict-ex conflict-data result))))))))

(defn rethrow-unique-conflict
  "Calls f. When it throws a SQLException for which (unique-violation? e) is
   true, rethrows it as a :s-exp.drip/unique-conflict ExceptionInfo with data."
  [unique-violation? data f]
  (try
    (f)
    (catch java.sql.SQLException e
      (if (unique-violation? e)
        (throw (unique-conflict-ex data e))
        (throw e)))))

;; ---------------------------------------------------------------------------
;; Multi-row batch insert (MariaDB / SQLite)
;; ---------------------------------------------------------------------------

;; Rows per INSERT statement: keeps statements under driver placeholder
;; limits (MariaDB server-side prepare: 65535) and SQLite's statement size cap.
(def ^:private insert-chunk-size 1000)

(def ^:private insert-many-columns
  (str "(attempt, attempted_by, encoded_args, errors, kind, max_attempts, metadata, "
       "priority, queue, scheduled_at, state, tags, ephemeral, timeout_ms, ttl_ms)"))

(def ^:private insert-many-row
  "(0, '[]', ?, '[]', ?, ?, ?, ?, ?, ?, ?, '[]', 0, ?, ?)")

(defn- insert-many-params [encode-ts ^Instant now [kind args opts]]
  (let [opts (merge job/default-insert-opts opts)
        ^Instant scheduled-at (or (:scheduled-at opts) now)]
    [(->json-str args)
     kind
     (int (:max-attempts opts))
     (->json-str (:metadata opts))
     (int (:priority opts))
     (:queue opts)
     (encode-ts scheduled-at)
     (if (.isAfter scheduled-at now) "scheduled" "available")
     (job/duration-ms (:timeout opts))
     (job/duration-ms (:ttl opts))]))

(defn insert-many-rows!
  "Inserts job-specs ([kind args opts] tuples) with chunked multi-row INSERTs
   in a single transaction. encode-ts converts an Instant to the dialect's
   timestamp parameter. Returns the number of rows inserted."
  ^long [ds encode-ts job-specs]
  (let [now (Instant/now)]
    (jdbc/with-transaction [tx ds]
      (reduce (fn [^long n chunk]
                (let [sql (str "INSERT INTO drip_job " insert-many-columns " VALUES "
                               (str/join "," (repeat (count chunk) insert-many-row)))
                      params (mapcat #(insert-many-params encode-ts now %) chunk)]
                  (+ n (long (:next.jdbc/update-count
                              (jdbc/execute-one! tx (into [sql] params)))))))
              0
              (partition-all insert-chunk-size job-specs)))))

;; ---------------------------------------------------------------------------
;; DDL / Migration
;; ---------------------------------------------------------------------------

(defn migrate!
  "Runs pending migrations against the datasource. Idempotent - safe to call
   on every application startup. Accepts a Client record (from make-client)."
  [c]
  (migratus/migrate (assoc (client/migration-config c)
                           :store :database
                           :db {:datasource (:ds c)})))
