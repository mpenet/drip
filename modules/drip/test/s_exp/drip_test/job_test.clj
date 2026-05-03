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
    (is (nil? (db/compute-unique-key "kind" (db/->json {}) "q" (Instant/now) nil))))

  (testing "non-nil unique-opts returns 32 bytes"
    (let [k (db/compute-unique-key "kind" (db/->json {:x 1}) "q" (Instant/now)
                                   {:by-args? true :by-period nil
                                    :by-queue? false :by-state nil})]
      (is (bytes? k))
      (is (= 32 (alength ^bytes k)))))

  (testing "same inputs produce same key"
    (let [now (Instant/now)
          args (db/->json {:x 1})
          opts {:by-args? true :by-period nil :by-queue? false :by-state nil}
          k1 (db/compute-unique-key "kind" args "q" now opts)
          k2 (db/compute-unique-key "kind" args "q" now opts)]
      (is (java.util.Arrays/equals ^bytes k1 ^bytes k2))))

  (testing "different args produce different keys"
    (let [now (Instant/now)
          opts {:by-args? true :by-period nil :by-queue? false :by-state nil}
          k1 (db/compute-unique-key "kind" (db/->json {:x 1}) "q" now opts)
          k2 (db/compute-unique-key "kind" (db/->json {:x 2}) "q" now opts)]
      (is (not (java.util.Arrays/equals ^bytes k1 ^bytes k2)))))

  (testing "by-period: keys in same window are equal"
    (let [opts {:by-args? false :by-period 3600000 :by-queue? false :by-state nil}
          now (Instant/now)
          t2 (.plusSeconds now 1)
          k1 (db/compute-unique-key "k" (db/->json {}) "q" now opts)
          k2 (db/compute-unique-key "k" (db/->json {}) "q" t2 opts)]
      (is (java.util.Arrays/equals ^bytes k1 ^bytes k2))))

  (testing "by-queue: different queues produce different keys"
    (let [now (Instant/now)
          opts {:by-args? false :by-period nil :by-queue? true :by-state nil}
          k1 (db/compute-unique-key "k" (db/->json {}) "q1" now opts)
          k2 (db/compute-unique-key "k" (db/->json {}) "q2" now opts)]
      (is (not (java.util.Arrays/equals ^bytes k1 ^bytes k2))))))

(deftest retry-policy-test
  (testing "default-retry-policy returns Instant in the future"
    (let [before (Instant/now)
          next-at (job/default-retry-policy 1)]
      (is (.isAfter ^Instant next-at before)))))
