(ns s-exp.drip-ui.db
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)))

(defn make-datasource
  "Creates a HikariCP DataSource from a JDBC URL."
  ^HikariDataSource [^String jdbc-url]
  (let [cfg (HikariConfig.)]
    (.setJdbcUrl cfg jdbc-url)
    (.setMaximumPoolSize cfg 5)
    (.setMinimumIdle cfg 1)
    (HikariDataSource. cfg)))

(defn close!
  "Closes the DataSource and releases all connections."
  [^HikariDataSource ds]
  (.close ds))
