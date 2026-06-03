(ns s-exp.drip-test.job-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [s-exp.drip-test.fixture :refer [with-db]]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job])
  (:import (java.time Instant)))

(use-fixtures :each with-db)

;; ---------------------------------------------------------------------------
;; Unit tests — no DB
;; ---------------------------------------------------------------------------

(deftest bitmask-roundtrip
  (testing "all states encode/decode correctly"
    (is (= job/states (job/bitmask->states (job/states->bitmask job/states)))))

  (testing "subset roundtrip"
    (let [s #{:available :running :scheduled}]
      (is (= s (job/bitmask->states (job/states->bitmask s))))))

  (testing "empty set"
    (is (= #{} (job/bitmask->states 0))))

  (testing "state-in-bitmask? true"
    (let [mask (job/states->bitmask #{:available :running})]
      (is (job/state-in-bitmask? :available mask))
      (is (job/state-in-bitmask? :running mask))))

  (testing "state-in-bitmask? false"
    (let [mask (job/states->bitmask #{:available})]
      (is (not (job/state-in-bitmask? :running mask)))))

  (testing "unknown state keyword returns false, does not throw"
    (is (not (job/state-in-bitmask? :nonexistent (job/states->bitmask #{:running}))))))

(deftest default-insert-opts
  (testing "default fields"
    (is (= "default" (:queue job/default-insert-opts)))
    (is (= 25 (:max-attempts job/default-insert-opts)))
    (is (= 1 (:priority job/default-insert-opts)))
    (is (= [] (:tags job/default-insert-opts)))
    (is (= {} (:metadata job/default-insert-opts)))))

(deftest compute-unique-key-test
  (testing "nil unique-opts returns nil"
    (is (nil? (db/compute-unique-key "kind" {} "q" (Instant/now) nil))))

  (testing "non-nil unique-opts returns 32 bytes"
    (let [k (db/compute-unique-key "kind" {:x 1} "q" (Instant/now)
                                   {:by-args true :by-period nil
                                    :by-queue false :by-state nil})]
      (is (bytes? k))
      (is (= 32 (alength ^bytes k)))))

  (testing "same inputs produce same key"
    (let [now (Instant/now)
          opts {:by-args true :by-period nil :by-queue false :by-state nil}
          k1 (db/compute-unique-key "kind" {:x 1} "q" now opts)
          k2 (db/compute-unique-key "kind" {:x 1} "q" now opts)]
      (is (java.util.Arrays/equals ^bytes k1 ^bytes k2))))

  (testing "different args produce different keys"
    (let [now (Instant/now)
          opts {:by-args true :by-period nil :by-queue false :by-state nil}
          k1 (db/compute-unique-key "kind" {:x 1} "q" now opts)
          k2 (db/compute-unique-key "kind" {:x 2} "q" now opts)]
      (is (not (java.util.Arrays/equals ^bytes k1 ^bytes k2)))))

  (testing "by-period: keys in same window are equal"
    (let [opts {:by-args false :by-period 3600000 :by-queue false :by-state nil}
          now (Instant/now)
          t2 (.plusSeconds now 1)
          k1 (db/compute-unique-key "k" {} "q" now opts)
          k2 (db/compute-unique-key "k" {} "q" t2 opts)]
      (is (java.util.Arrays/equals ^bytes k1 ^bytes k2))))

  (testing "by-queue: different queues produce different keys"
    (let [now (Instant/now)
          opts {:by-args false :by-period nil :by-queue true :by-state nil}
          k1 (db/compute-unique-key "k" {} "q1" now opts)
          k2 (db/compute-unique-key "k" {} "q2" now opts)]
      (is (not (java.util.Arrays/equals ^bytes k1 ^bytes k2)))))

  (testing "by-keys: only selected keys included in hash"
    (let [now (Instant/now)
          opts {:by-keys [:a :b]}
          args-full {:a 1 :b 2 :c 99}
          args-diff-c {:a 1 :b 2 :c 0}
          args-diff-b {:a 1 :b 9 :c 99}
          k-full (db/compute-unique-key "k" args-full "q" now opts)
          k-diff-c (db/compute-unique-key "k" args-diff-c "q" now opts)
          k-diff-b (db/compute-unique-key "k" args-diff-b "q" now opts)]
      ;; :c differs but is not in by-keys → same key
      (is (java.util.Arrays/equals ^bytes k-full ^bytes k-diff-c))
      ;; :b differs and is in by-keys → different key
      (is (not (java.util.Arrays/equals ^bytes k-full ^bytes k-diff-b)))))

  (testing "by-keys: key order in by-keys does not affect hash"
    (let [now (Instant/now)
          args {:a 1 :b 2}
          k1 (db/compute-unique-key "k" args "q" now {:by-keys [:a :b]})
          k2 (db/compute-unique-key "k" args "q" now {:by-keys [:b :a]})]
      (is (java.util.Arrays/equals ^bytes k1 ^bytes k2))))

  (testing "exclude-kind: different kinds produce same key when exclude-kind is true"
    (let [now (Instant/now)
          opts {:by-args true :exclude-kind true}
          k1 (db/compute-unique-key "kind-a" {:x 1} "q" now opts)
          k2 (db/compute-unique-key "kind-b" {:x 1} "q" now opts)]
      (is (java.util.Arrays/equals ^bytes k1 ^bytes k2))))

  (testing "exclude-kind: same args without exclude-kind differ by kind"
    (let [now (Instant/now)
          opts {:by-args true}
          k1 (db/compute-unique-key "kind-a" {:x 1} "q" now opts)
          k2 (db/compute-unique-key "kind-b" {:x 1} "q" now opts)]
      (is (not (java.util.Arrays/equals ^bytes k1 ^bytes k2)))))

  (testing "by-args: key order in args map does not affect hash"
    (let [now (Instant/now)
          opts {:by-args true}
          k1 (db/compute-unique-key "k" {:a 1 :b 2} "q" now opts)
          k2 (db/compute-unique-key "k" {:b 2 :a 1} "q" now opts)]
      (is (java.util.Arrays/equals ^bytes k1 ^bytes k2)))))

(deftest retry-policy-test
  (testing "default-retry-policy returns Instant in the future"
    (let [before (Instant/now)
          next-at (job/default-retry-policy 1)]
      (is (.isAfter ^Instant next-at before))))

  (testing "default-retry-policy increases delay with attempt"
    (let [delay1 (.toEpochMilli ^Instant (job/default-retry-policy 1))
          delay5 (.toEpochMilli ^Instant (job/default-retry-policy 5))]
      (is (< delay1 delay5))))

  (testing "constant-retry-policy always returns roughly the same delay"
    (let [policy (job/constant-retry-policy "5s")
          before (.toEpochMilli (Instant/now))
          t1 (.toEpochMilli ^Instant (policy 1))
          t2 (.toEpochMilli ^Instant (policy 10))
          t3 (.toEpochMilli ^Instant (policy 100))]
      (is (< (Math/abs (- t1 t2)) 200))
      (is (< (Math/abs (- t1 t3)) 200))
      (is (> t1 before))))

  (testing "constant-retry-policy with jitter stays within expected range"
    (let [delay-ms 10000
          jitter 0.2
          policy (job/constant-retry-policy "10s" :jitter jitter)
          now (.toEpochMilli (Instant/now))
          samples (repeatedly 20 #(.toEpochMilli ^Instant (policy 1)))
          lo (+ now (long (* delay-ms (- 1 jitter))))
          hi (+ now (long (* delay-ms (+ 1 jitter))))]
      (is (every? #(and (>= % lo) (<= % hi)) samples))))

  (testing "constant-retry-policy accepts raw ms number"
    (let [policy (job/constant-retry-policy 5000)
          before (.toEpochMilli (Instant/now))
          t1 (.toEpochMilli ^Instant (policy 1))]
      (is (> t1 before))))

  (testing "linear-retry-policy delay grows linearly with attempt"
    (let [policy (job/linear-retry-policy "1s")
          now (.toEpochMilli (Instant/now))
          t1 (- (.toEpochMilli ^Instant (policy 1)) now)
          t3 (- (.toEpochMilli ^Instant (policy 3)) now)]
      (is (< t1 t3))
      (is (< (Math/abs (- t1 1000)) 50))
      (is (< (Math/abs (- t3 3000)) 50))))

  (testing "linear-retry-policy respects max cap"
    (let [policy (job/linear-retry-policy "1s" :max "2500ms")
          now (.toEpochMilli (Instant/now))
          t10 (- (.toEpochMilli ^Instant (policy 10)) now)]
      (is (<= t10 2500))))

  (testing "exponential-retry-policy delay grows exponentially"
    (let [policy (job/exponential-retry-policy "1s" :multiplier 2.0 :max Long/MAX_VALUE :jitter 0.0)
          now (.toEpochMilli (Instant/now))
          t1 (- (.toEpochMilli ^Instant (policy 1)) now)
          t2 (- (.toEpochMilli ^Instant (policy 2)) now)
          t3 (- (.toEpochMilli ^Instant (policy 3)) now)]
      (is (< t1 t2))
      (is (< t2 t3))
      (is (< (Math/abs (- t1 1000)) 50))
      (is (< (Math/abs (- t2 2000)) 50))
      (is (< (Math/abs (- t3 4000)) 50))))

  (testing "exponential-retry-policy respects max cap"
    (let [policy (job/exponential-retry-policy "1s" :multiplier 2.0 :max "5s" :jitter 0.0)
          now (.toEpochMilli (Instant/now))
          t10 (- (.toEpochMilli ^Instant (policy 10)) now)]
      (is (<= t10 5000))))

  (testing "immediate-retry-policy returns Instant at or before now"
    (let [policy (job/immediate-retry-policy)
          before (Instant/now)
          next-at (policy 1)
          after (Instant/now)]
      (is (not (.isAfter ^Instant next-at after)))))

  (testing "immediate-retry-policy ignores attempt number"
    (let [policy (job/immediate-retry-policy)
          t1 (.toEpochMilli ^Instant (policy 1))
          t25 (.toEpochMilli ^Instant (policy 25))]
      (is (< (Math/abs (- t1 t25)) 100)))))
