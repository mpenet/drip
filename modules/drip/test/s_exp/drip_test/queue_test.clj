(ns s-exp.drip-test.queue-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc :as jdbc]
            [s-exp.drip :as drip]
            [s-exp.drip-test.fixture :refer [*client* with-db]]
            [s-exp.drip.client :as drip-client]))

(use-fixtures :each with-db)

;; ---------------------------------------------------------------------------
;; Queue management
;; ---------------------------------------------------------------------------

(deftest queue-upsert-test
  (testing "upsert creates queue"
    (drip/upsert-queue *client* "test-q" {:desc "hello"})
    (let [qs (drip/list-queues *client*)]
      (is (some #(= "test-q" (:name %)) qs))))

  (testing "upsert is idempotent"
    (drip/upsert-queue *client* "idem-q" {:v 1})
    (drip/upsert-queue *client* "idem-q" {:v 2})
    (let [qs (drip/list-queues *client*)
          q (first (filter #(= "idem-q" (:name %)) qs))]
      (is (some? q)))))

(deftest queue-pause-resume-test
  (testing "paused queue blocks fetch"
    (drip/upsert-queue *client* "pauseq" {})
    (drip/insert-job *client* "k" {} {:queue "pauseq"})
    (drip/pause-queue *client* "pauseq")
    (is (jdbc/with-transaction [tx (:ds *client*)]
          (drip-client/queue-paused? *client* tx "pauseq")))
    (let [jobs (when-not (jdbc/with-transaction [tx (:ds *client*)]
                           (drip-client/queue-paused? *client* tx "pauseq"))
                 (drip/fetch-jobs *client* "pauseq" "w" :limit 10))]
      (is (nil? jobs))))

  (testing "resumed queue allows fetch"
    (drip/resume-queue *client* "pauseq")
    (is (not (jdbc/with-transaction [tx (:ds *client*)]
               (drip-client/queue-paused? *client* tx "pauseq"))))
    (let [jobs (drip/fetch-jobs *client* "pauseq" "w" :limit 10)]
      (is (= 1 (count jobs)))))

  (testing "queue-paused? false for unknown queue"
    (is (not (jdbc/with-transaction [tx (:ds *client*)]
               (drip-client/queue-paused? *client* tx "no-such-queue"))))))

;; ---------------------------------------------------------------------------
;; Explicit-tx queue variants
;; ---------------------------------------------------------------------------

(deftest explicit-tx-queue-variants
  (testing "upsert-queue! with explicit tx"
    (jdbc/with-transaction [tx (:ds *client*)]
      (drip/upsert-queue! *client* tx "etx-q" {:v 1}))
    (is (some #(= "etx-q" (:name %)) (drip/list-queues *client*))))

  (testing "pause-queue! and resume-queue! with explicit tx"
    (drip/upsert-queue *client* "etx-pq" {})
    (jdbc/with-transaction [tx (:ds *client*)]
      (drip/pause-queue! *client* tx "etx-pq"))
    (is (jdbc/with-transaction [tx (:ds *client*)]
          (drip-client/queue-paused? *client* tx "etx-pq")))
    (jdbc/with-transaction [tx (:ds *client*)]
      (drip/resume-queue! *client* tx "etx-pq"))
    (is (not (jdbc/with-transaction [tx (:ds *client*)]
               (drip-client/queue-paused? *client* tx "etx-pq")))))

  (testing "list-queues! with explicit tx"
    (drip/upsert-queue *client* "etx-lq" {})
    (let [qs (jdbc/with-transaction [tx (:ds *client*)]
               (drip/list-queues! *client* tx))]
      (is (some #(= "etx-lq" (:name %)) qs)))))
