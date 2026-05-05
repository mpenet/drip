(ns s-exp.drip-cli.db
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)))

(defn make-datasource ^HikariDataSource [^String jdbc-url]
  (let [cfg (HikariConfig.)]
    (.setJdbcUrl cfg jdbc-url)
    (.setMaximumPoolSize cfg 3)
    (.setConnectionTimeout cfg 5000)
    (HikariDataSource. cfg)))

(defn close! [^HikariDataSource ds]
  (.close ds))

(defn detect-db [^String jdbc-url]
  (cond
    (re-find #"(?i)postgresql|postgres" jdbc-url) :postgres
    (re-find #"(?i)mariadb|mysql" jdbc-url) :mariadb
    (re-find #"(?i)sqlite" jdbc-url) :sqlite
    :else (throw (ex-info (str "Cannot detect DB type from JDBC URL: " jdbc-url) {}))))

(defn make-client [^HikariDataSource ds db-type]
  (let [ns-sym (symbol (str "s-exp.drip.client." (name db-type)))]
    (require ns-sym)
    ((ns-resolve (find-ns ns-sym) 'make-client) ds)))
