(ns s-exp.drip.job
  (:require [s-exp.duration :as duration]))

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
   :metadata {}
   :ephemeral false})

;; ---------------------------------------------------------------------------
;; Retry policy
;; ---------------------------------------------------------------------------

(defn- add-jitter
  "Adds ±jitter-factor fractional jitter to ms. Returns long."
  ^long [^long ms ^double jitter-factor]
  (let [jitter (* ms jitter-factor (- (rand) 0.5))]
    (max 0 (long (+ ms jitter)))))

(defn constant-retry-policy
  "Returns a retry policy fn that always waits `delay` between retries.
   `delay` is a duration value: string (e.g. \"30s\", \"2m\") or number of milliseconds.
   Options:
     :jitter - fractional ± jitter applied to delay (default 0.0)
   Returns delay in milliseconds."
  [delay & {:keys [jitter] :or {jitter 0.0}}]
  (let [ms (long (duration/duration delay))]
    (fn [_attempt]
      (add-jitter ms jitter))))

(defn linear-retry-policy
  "Returns a retry policy fn that waits `base` * attempt.
   `base` is a duration value: string (e.g. \"10s\") or number of milliseconds.
   Options:
     :max    - duration cap on computed delay (default unbounded)
     :jitter - fractional ± jitter applied to delay (default 0.0)
   Returns delay in milliseconds."
  [base & {:keys [max jitter] :or {max Long/MAX_VALUE jitter 0.0}}]
  (let [base-ms (long (duration/duration base))
        max-ms (long (duration/duration max))]
    (fn [^long attempt]
      (add-jitter (min (* base-ms attempt) max-ms) jitter))))

(defn exponential-retry-policy
  "Returns a retry policy fn with configurable exponential backoff.
   Waits `base` * multiplier^(attempt-1), capped at `max`.
   `base` is a duration value: string (e.g. \"1s\") or number of milliseconds.
   Options:
     :multiplier - growth factor (default 2.0)
     :max        - duration cap on computed delay (default \"1h\")
     :jitter     - fractional ± jitter applied to delay (default 0.1)
   Returns delay in milliseconds."
  [base & {:keys [multiplier max jitter] :or {multiplier 2.0 max "1h" jitter 0.1}}]
  (let [base-ms (long (duration/duration base))
        max-ms (long (duration/duration max))]
    (fn [^long attempt]
      (let [raw (long (* base-ms (Math/pow multiplier (dec attempt))))]
        (add-jitter (min raw max-ms) jitter)))))

(def default-retry-policy
  "Exponential backoff: base 1s, multiplier 2, max 1h, ±10% jitter.
   Delays: ~1s, ~2s, ~4s, ~8s, ~16s, ~32s, ... capped at ~1h."
  (exponential-retry-policy "1s"))

(defn immediate-retry-policy
  "Returns a retry policy fn that retries immediately with no delay.
   Useful for testing or jobs that should be reattempted without waiting.
   Returns 0 milliseconds."
  []
  (fn [_attempt] 0))
