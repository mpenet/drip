(ns s-exp.drip-ui.format
  (:import (java.time Instant ZoneOffset)
           (java.time.format DateTimeFormatter)))

(def ^:private dt-fmt
  (-> (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss")
      (.withZone ZoneOffset/UTC)))

(defn fmt-instant
  "Formats a java.time.Instant as a UTC datetime string, or \"-\" if nil."
  [inst]
  (if (instance? Instant inst)
    (.format dt-fmt ^Instant inst)
    "-"))

(defn fmt-duration-ago
  "Returns a human-readable relative time string for an Instant, e.g. '2m ago'."
  [inst]
  (when (instance? Instant inst)
    (let [now (Instant/now)
          secs (.getEpochSecond now)
          then (.getEpochSecond ^Instant inst)
          diff (- secs then)]
      (cond
        (< diff 60) (str diff "s ago")
        (< diff 3600) (str (quot diff 60) "m ago")
        (< diff 86400) (str (quot diff 3600) "h ago")
        :else (str (quot diff 86400) "d ago")))))

(def state-badge-class
  {:available "badge-available"
   :running "badge-running"
   :completed "badge-completed"
   :failed "badge-failed"
   :retryable "badge-retryable"
   :scheduled "badge-scheduled"
   :cancelled "badge-cancelled"
   :discarded "badge-discarded"
   :pending "badge-pending"})

(defn state-badge
  "Returns a hiccup span element for the given state keyword."
  [state]
  [:span {:class (str "badge " (get state-badge-class state "badge-unknown"))}
   (name state)])
