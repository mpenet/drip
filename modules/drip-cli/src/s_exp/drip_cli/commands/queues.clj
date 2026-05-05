(ns s-exp.drip-cli.commands.queues
  (:require [s-exp.drip-cli.format :as fmt]
            [s-exp.drip :as drip]))

(defn list-queues [client {:keys [format]}]
  (let [queues (drip/list-queues client)]
    (if (= format "json")
      (fmt/print-json queues)
      (fmt/print-table fmt/queue-columns queues))))

(defn pause-queue [client {:keys [name]}]
  (when-not name
    (println "Error: --name required")
    (System/exit 1))
  (drip/pause-queue client name)
  (println (str "Paused queue: " name)))

(defn resume-queue [client {:keys [name]}]
  (when-not name
    (println "Error: --name required")
    (System/exit 1))
  (drip/resume-queue client name)
  (println (str "Resumed queue: " name)))
