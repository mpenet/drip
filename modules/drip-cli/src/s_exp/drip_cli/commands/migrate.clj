(ns s-exp.drip-cli.commands.migrate
  (:require [s-exp.drip :as drip]))

(defn migrate [client _opts]
  (println "Running migrations...")
  (drip/migrate! client)
  (println "Migrations complete."))
