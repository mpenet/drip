(ns s-exp.drip-cli.commands.jobs
  (:require [clojure.string :as str]
            [s-exp.drip :as drip]
            [s-exp.drip-cli.format :as fmt])
  (:import (java.time Instant)))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- parse-instant [s]
  (when s (Instant/parse s)))

(defn- parse-states [s]
  (when s (map keyword (str/split s #","))))

(defn- parse-kinds [s]
  (when s (str/split s #",")))

(defn- parse-queues [s]
  (when s (str/split s #",")))

(defn- build-list-opts [{:keys [state states kind kinds queue queues
                                limit after
                                created-after created-before
                                scheduled-after scheduled-before]}]
  (cond-> {}
    state (assoc :state (keyword state))
    states (assoc :states (parse-states states))
    kind (assoc :kind kind)
    kinds (assoc :kinds (parse-kinds kinds))
    queue (assoc :queue queue)
    queues (assoc :queues (parse-queues queues))
    limit (assoc :limit (Long/parseLong limit))
    after (assoc :after (Long/parseLong after))
    created-after (assoc :created-after (parse-instant created-after))
    created-before (assoc :created-before (parse-instant created-before))
    scheduled-after (assoc :scheduled-after (parse-instant scheduled-after))
    scheduled-before (assoc :scheduled-before (parse-instant scheduled-before))))

;; ---------------------------------------------------------------------------
;; Commands
;; ---------------------------------------------------------------------------

(defn list-jobs [client {:keys [format] :as opts}]
  (let [jobs (drip/list-jobs client (build-list-opts opts))]
    (if (= format "json")
      (fmt/print-json jobs)
      (fmt/print-table fmt/job-columns jobs))))

(defn get-job [client {:keys [id format]}]
  (let [job-id (Long/parseLong id)
        job (drip/get-job client job-id)]
    (if (nil? job)
      (do (println (str "Job not found: " id)) (System/exit 1))
      (if (= format "json")
        (fmt/print-json job)
        (fmt/print-job-detail job)))))

(defn cancel-job [client {:keys [id]}]
  (let [job-id (Long/parseLong id)
        job (drip/cancel-job client job-id)]
    (if job
      (println (str "Cancelled job " id))
      (println (str "Job not found or not cancellable: " id)))))

(defn retry-job [client {:keys [id]}]
  (let [job-id (Long/parseLong id)
        job (drip/retry-job client job-id)]
    (if job
      (println (str "Retrying job " id))
      (println (str "Job not found: " id)))))

(defn discard-job [client {:keys [id]}]
  (let [job-id (Long/parseLong id)
        job (drip/discard-job client job-id)]
    (if job
      (println (str "Discarded job " id))
      (println (str "Job not found: " id)))))

(defn delete-job [client {:keys [id]}]
  (let [job-id (Long/parseLong id)
        job (drip/delete-job client job-id)]
    (if job
      (println (str "Deleted job " id))
      (println (str "Job not found: " id)))))

(defn delete-jobs [client {:keys [states kinds queues created-before finalized-before] :as _opts}]
  (when-not states
    (println "Error: --states required for delete-jobs")
    (System/exit 1))
  (let [opts (cond-> {:states (map keyword (str/split states #","))}
               kinds (assoc :kinds (parse-kinds kinds))
               queues (assoc :queues (parse-queues queues))
               created-before (assoc :created-before (parse-instant created-before))
               finalized-before (assoc :finalized-before (parse-instant finalized-before)))
        n (drip/delete-jobs client opts)]
    (println (str "Deleted " n " jobs"))))
