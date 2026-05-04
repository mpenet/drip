(ns s-exp.drip-ui
  (:require [s-exp.drip-ui.db :as db]
            [s-exp.drip-ui.routes :as routes]
            [s-exp.hirundo :as hirundo])
  (:import (com.zaxxer.hikari HikariDataSource)))

(defn- make-drip-client
  "Creates a drip client appropriate for the given JDBC URL dialect."
  [^HikariDataSource ds ^String jdbc-url]
  (cond
    (re-find #"(?i)mariadb" jdbc-url)
    (do (require 's-exp.drip.client.mariadb)
        ((ns-resolve 's-exp.drip.client.mariadb 'make-client) ds))

    (re-find #"(?i)postgresql|postgres" jdbc-url)
    (do (require 's-exp.drip.client.postgres)
        ((ns-resolve 's-exp.drip.client.postgres 'make-client) ds))

    (re-find #"(?i)sqlite" jdbc-url)
    (do (require 's-exp.drip.client.sqlite)
        ((ns-resolve 's-exp.drip.client.sqlite 'make-client) ds))

    :else
    (throw (ex-info "Unsupported JDBC URL dialect. Expected postgresql, mariadb, or sqlite."
                    {:jdbc-url jdbc-url}))))

(defn -main [& _args]
  (let [jdbc-url (or (System/getenv "DRIP_JDBC_URL")
                     (throw (ex-info "DRIP_JDBC_URL environment variable is not set." {})))
        port (Long/parseLong (or (System/getenv "DRIP_PORT") "8080"))
        ds (db/make-datasource jdbc-url)
        client (make-drip-client ds jdbc-url)
        server (hirundo/start! {:http-handler (routes/handler client) :port port})]
    (println (str "drip-ui running at http://localhost:" port))
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn []
                                           (hirundo/stop! server)
                                           (db/close! ds))))
    @(promise)))
