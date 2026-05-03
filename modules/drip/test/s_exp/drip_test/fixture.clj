(ns s-exp.drip-test.fixture
  (:require [next.jdbc :as jdbc]
            [s-exp.drip :as drip])
  (:import (java.io File)))

;; ---------------------------------------------------------------------------
;; Fixture — selects DB via DRIP_TEST_DB env var (or infers from classpath)
;;
;;   DRIP_TEST_DB=sqlite   — temp file, no external DB (default when driver on classpath)
;;   DRIP_TEST_DB=mariadb  — requires DRIP_TEST_JDBC_URL
;;   DRIP_TEST_DB=postgres — requires DRIP_TEST_JDBC_URL
;;
;;   Example (MariaDB):
;;     DRIP_TEST_DB=mariadb \
;;     DRIP_TEST_JDBC_URL="jdbc:mariadb://localhost:3306/drip_test?user=root&password=root" \
;;     clojure -M:test-mariadb -m cognitect.test-runner
;; ---------------------------------------------------------------------------

(def ^:dynamic *client* nil)

(defn- driver-available? [class-name]
  (try (Class/forName class-name) true (catch ClassNotFoundException _ false)))

(defn- infer-db []
  (cond
    (driver-available? "org.sqlite.JDBC") "sqlite"
    (driver-available? "org.mariadb.jdbc.Driver") "mariadb"
    (driver-available? "org.postgresql.Driver") "postgres"
    :else (throw (IllegalStateException. "No supported JDBC driver on classpath"))))

(defn make-client-for-env []
  (let [db (or (System/getenv "DRIP_TEST_DB") (infer-db))]
    (case db
      "sqlite"
      (let [ns-sym 's-exp.drip.client.sqlite
            _ (require ns-sym)
            make-fn (ns-resolve (find-ns ns-sym) 'make-client)
            tmp (File/createTempFile "drip-test-" ".db")]
        (.deleteOnExit tmp)
        {:client (make-fn
                  (jdbc/get-datasource
                   (str "jdbc:sqlite:" (.getAbsolutePath tmp)
                        "?journal_mode=WAL&busy_timeout=5000&mode=memory&cache=shared")))
         :cleanup #(.delete tmp)})

      ("mariadb" "postgres")
      (let [url (or (System/getenv "DRIP_TEST_JDBC_URL")
                    (throw (IllegalStateException.
                            (str "DRIP_TEST_JDBC_URL required for " db))))
            ns-sym (symbol (str "s-exp.drip.client." db))
            _ (require ns-sym)
            make-fn (ns-resolve (find-ns ns-sym) 'make-client)
            ds (jdbc/get-datasource url)
            c (make-fn ds)]
        {:client c
         :cleanup (fn []
                    (jdbc/execute! (:ds c) ["DELETE FROM drip_job"])
                    (jdbc/execute! (:ds c) ["DELETE FROM drip_queue"]))})

      (throw (IllegalArgumentException. (str "Unknown DRIP_TEST_DB: " db))))))

(defn with-db [f]
  (let [{:keys [client cleanup]} (make-client-for-env)]
    (drip/migrate! client)
    (binding [*client* client]
      (f))
    (cleanup)))
