(ns s-exp.drip.db
  (:require [jsonista.core :as json]
            [migratus.core :as migratus]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [s-exp.drip.client :as client]
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

;; ---------------------------------------------------------------------------
;; Unique key computation (shared across standard dialects)
;; ---------------------------------------------------------------------------

(defn- sha256 ^bytes [^String s]
  (let [^MessageDigest md (MessageDigest/getInstance "SHA-256")]
    (.digest md (.getBytes s "UTF-8"))))

(defn- period-floor-ms ^long [^long epoch-ms ^long period-ms]
  (* (quot epoch-ms period-ms) period-ms))

(defn- args->hex ^String [args]
  (.toString (BigInteger. 1 (->json args)) 16))

(defn compute-unique-key
  "Computes a 32-byte SHA-256 unique key for a job.
   Returns nil when unique-opts is nil (no uniqueness constraint).
   args is the raw (pre-serialization) args map."
  ^bytes [kind args queue ^Instant now unique-opts]
  (when unique-opts
    (let [{:keys [by-args by-keys by-period by-queue]} unique-opts
          sb (StringBuilder.)]
      (.append sb "kind=")
      (.append sb ^String kind)
      (cond
        (seq by-keys)
        (do (.append sb "&args=")
            (.append sb (args->hex (into (sorted-map) (select-keys args by-keys)))))
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
;; DDL / Migration
;; ---------------------------------------------------------------------------

(defn migrate!
  "Runs pending migrations against the datasource. Idempotent - safe to call
   on every application startup. Accepts a Client record (from make-client)."
  [c]
  (migratus/migrate (assoc (client/migration-config c)
                           :store :database
                           :db {:datasource (:ds c)})))
