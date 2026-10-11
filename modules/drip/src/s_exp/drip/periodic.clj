(ns s-exp.drip.periodic
  (:require [clojure.tools.logging :as log]
            [s-exp.drip.client :as client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.duration :as duration])
  (:import (java.util.concurrent Executors ScheduledExecutorService TimeUnit)))

(defn- period-ms ^long [period]
  (long (duration/duration period)))

(defn- periodic-unique-opts [period-ms queue]
  {:by-args false
   :by-period period-ms
   :by-queue (boolean queue)
   :by-state job/default-unique-states})

(defn- periodic-insert-opts [spec]
  (let [ms (period-ms (:period spec))]
    (merge (:opts spec)
           {:queue (or (:queue spec) "default")
            :unique-opts (periodic-unique-opts ms (:queue spec))})))

(defn start-periodic-jobs!
  "Schedules periodic job insertions for a sequence of spec maps.
   Each spec fires at the start of every period interval.
   Duplicate insertions within a period are skipped (unique per period).

   Spec keys:
     :kind   - job kind string
     :args   - job args map
     :period - period as ms number or duration string (e.g. \"1h\", \"30m\")
     :queue  - queue name (default \"default\")
     :opts   - extra insert opts map or nil

   `client` is a Client record (from make-client).
   Returns a ScheduledExecutorService. Stop with stop-periodic-jobs!."
  [c specs]
  (let [scheduler (Executors/newScheduledThreadPool (int (max 1 (count specs))))]
    (doseq [{:keys [kind args period] :as spec} specs]
      (.scheduleAtFixedRate
       scheduler
       ^Runnable (fn []
                   (try
                     (db/with-tx [tx c]
                       (client/insert-job! c tx kind args (periodic-insert-opts spec)))
                     (catch Exception t
                       (log/error t "drip: periodic insert error" {:kind kind}))))
       0
       (period-ms period)
       TimeUnit/MILLISECONDS))
    scheduler))

(defn stop-periodic-jobs!
  "Shuts down the periodic executor."
  [^ScheduledExecutorService scheduler]
  (.shutdown scheduler))
