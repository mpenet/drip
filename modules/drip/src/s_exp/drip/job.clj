(ns s-exp.drip.job
  (:import (java.time Instant)))

;; ---------------------------------------------------------------------------
;; States
;; ---------------------------------------------------------------------------

;; Bitmask positions match River Go's BIT(8) layout:
;;   bit7=available, bit6=cancelled, bit5=completed, bit4=discarded,
;;   bit3=pending,   bit2=retryable, bit1=running,   bit0=scheduled
(def state->bit
  {:available 7
   :cancelled 6
   :completed 5
   :discarded 4
   :pending 3
   :retryable 2
   :running 1
   :scheduled 0})

(def states (set (keys state->bit)))

(defn states->bitmask
  "Converts a collection of state keywords to an integer bitmask."
  ^long [state-coll]
  (reduce (fn [^long acc s]
            (bit-or acc (bit-shift-left 1 (get state->bit s))))
          0
          state-coll))

(defn bitmask->states
  "Converts an integer bitmask to a set of state keywords."
  [^long mask]
  (reduce-kv (fn [acc state bit]
               (if (pos? (bit-and mask (bit-shift-left 1 bit)))
                 (conj acc state)
                 acc))
             #{}
             state->bit))

(defn state-in-bitmask?
  "Returns true if the given state keyword is represented in the bitmask."
  [state ^long mask]
  (pos? (bit-and mask (bit-shift-left 1 (get state->bit state 0)))))

;; Default unique states: all active states (not finalized)
(def default-unique-states
  #{:available :pending :running :scheduled :retryable :completed})

(def default-insert-opts
  {:max-attempts 25
   :priority 1
   :queue "default"
   :tags []
   :metadata {}})

;; ---------------------------------------------------------------------------
;; Retry policy
;; ---------------------------------------------------------------------------

(defn- retry-delay-seconds
  "Exponential backoff: attempt^4 seconds ± 10% jitter."
  ^long [^long attempt]
  (let [base (Math/pow attempt 4)
        jitter (* base 0.1 (- (rand) 0.5))]
    (long (+ base jitter))))

(defn default-retry-policy
  "Returns a java.time.Instant for the next retry. attempt is 1-based."
  [^long attempt]
  (.plusSeconds (Instant/now) (retry-delay-seconds attempt)))
