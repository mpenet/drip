(ns s-exp.drip-test.db-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc :as jdbc]
            [s-exp.drip :as drip]
            [s-exp.drip-test.fixture :refer [*client* with-db]]
            [s-exp.drip.client :as drip-client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job])
  (:import (java.time Instant)))

(use-fixtures :each with-db)

;; ---------------------------------------------------------------------------
;; Migration
;; ---------------------------------------------------------------------------

(deftest migrate-idempotent
  (testing "calling migrate! twice does not throw"
    (drip/migrate! *client*)
    (drip/migrate! *client*))

  (testing "drip_job table exists after migrate!"
    (is (vector? (jdbc/execute! (:ds *client*) ["SELECT id FROM drip_job LIMIT 1"] db/jdbc-opts)))))

;; ---------------------------------------------------------------------------
;; insert-job
;; ---------------------------------------------------------------------------

(deftest insert-returns-job-record
  (testing "returns a Job with correct fields"
    (let [j (drip/insert-job *client* "send_email" {:to "a@b.com"} nil)]
      (is (int? (:id j)))
      (is (= "send_email" (:kind j)))
      (is (= :available (:state j)))
      (is (= 0 (:attempt j)))
      (is (= 1 (:priority j)))
      (is (= 25 (:max-attempts j)))
      (is (= "default" (:queue j)))
      (is (= {:to "a@b.com"} (:args j)))
      (is (= [] (:errors j)))
      (is (= [] (:tags j)))
      (is (= {} (:metadata j)))
      (is (nil? (:unique-key j)))
      (is (some? (:created-at j)))
      (is (some? (:scheduled-at j))))))

(deftest insert-with-all-opts
  (testing "priority, queue, max-attempts, tags, metadata"
    (let [j (drip/insert-job *client* "k" {:v 1}
                             {:priority 3
                              :queue "bulk"
                              :max-attempts 5
                              :tags ["t1" "t2"]
                              :metadata {:src "api"}})]
      (is (= 3 (:priority j)))
      (is (= "bulk" (:queue j)))
      (is (= 5 (:max-attempts j)))
      (is (= ["t1" "t2"] (:tags j)))
      (is (= {"src" "api"} (:metadata j)))))

  (testing "future scheduled-at → :scheduled state"
    (let [future (.plusSeconds (Instant/now) 3600)
          j (drip/insert-job *client* "k" {} {:scheduled-at future})]
      (is (= :scheduled (:state j)))))

  (testing "past scheduled-at → :available state"
    (let [past (.minusSeconds (Instant/now) 10)
          j (drip/insert-job *client* "k" {} {:scheduled-at past})]
      (is (= :available (:state j))))))

(deftest insert-preserves-args
  (testing "complex nested args round-trip"
    (let [args {:a 1 :b [1 2 3] :c {:d "hello"}}
          j (drip/insert-job *client* "k" args nil)]
      (is (= args (:args j))))))

;; ---------------------------------------------------------------------------
;; get-job / list-jobs
;; ---------------------------------------------------------------------------

(deftest get-job-test
  (testing "returns job by id"
    (let [j (drip/insert-job *client* "k" {} nil)
          fetched (drip/get-job *client* (:id j))]
      (is (= (:id j) (:id fetched)))
      (is (= :available (:state fetched)))))

  (testing "returns nil for unknown id"
    (is (nil? (drip/get-job *client* 999999)))))

(deftest list-jobs-test
  (testing "no filter returns all"
    (drip/insert-job *client* "k1" {} nil)
    (drip/insert-job *client* "k2" {} nil)
    (let [jobs (drip/list-jobs *client* {})]
      (is (>= (count jobs) 2))))

  (testing ":kind filter"
    (drip/insert-job *client* "unique_kind_xyz" {} nil)
    (let [jobs (drip/list-jobs *client* {:kind "unique_kind_xyz"})]
      (is (= 1 (count jobs)))
      (is (= "unique_kind_xyz" (:kind (first jobs))))))

  (testing ":state filter"
    (drip/insert-job *client* "s_test" {} nil)
    (let [jobs (drip/list-jobs *client* {:state :available :kind "s_test"})]
      (is (every? #(= :available (:state %)) jobs))))

  (testing ":queue filter"
    (drip/insert-job *client* "k" {} {:queue "special"})
    (let [jobs (drip/list-jobs *client* {:queue "special"})]
      (is (every? #(= "special" (:queue %)) jobs))))

  (testing ":limit respected"
    (dotimes [_ 5] (drip/insert-job *client* "lim_test" {} nil))
    (let [jobs (drip/list-jobs *client* {:kind "lim_test" :limit 2})]
      (is (= 2 (count jobs)))))

  (testing "unknown state throws"
    (is (thrown? IllegalArgumentException
                 (drip/list-jobs *client* {:state :bogus})))))

;; ---------------------------------------------------------------------------
;; fetch-jobs
;; ---------------------------------------------------------------------------

(deftest fetch-jobs-claims-jobs
  (testing "claims available jobs and marks them :running"
    (drip/insert-job *client* "k" {:n 1} nil)
    (drip/insert-job *client* "k" {:n 2} nil)
    (let [jobs (drip/fetch-jobs *client* "default" "w1" :limit 10)]
      (is (= 2 (count jobs)))
      (is (every? #(= :running (:state %)) jobs))
      (is (every? #(= 1 (:attempt %)) jobs))
      (is (every? #(= ["w1"] (:attempted-by %)) jobs))))

  (testing "second fetch from same queue returns empty"
    (let [more (drip/fetch-jobs *client* "default" "w2" :limit 10)]
      (is (empty? more))))

  (testing "limit respected"
    (dotimes [_ 5] (drip/insert-job *client* "lk" {} nil))
    (let [jobs (drip/fetch-jobs *client* "default" "w" :limit 3)]
      (is (<= (count jobs) 3))))

  (testing "fetches from correct queue only"
    (drip/insert-job *client* "k" {} {:queue "other"})
    (let [jobs (drip/fetch-jobs *client* "other" "w" :limit 10)]
      (is (every? #(= "other" (:queue %)) jobs)))))

(deftest fetch-scheduled-job-not-returned
  (testing "scheduled job (future scheduled_at) not returned by fetch"
    (drip/insert-job *client* "k" {} {:scheduled-at (.plusSeconds (Instant/now) 3600)})
    (let [jobs (drip/fetch-jobs *client* "default" "w" :limit 10)]
      (is (every? #(not= :scheduled (:state %)) jobs)))))

;; ---------------------------------------------------------------------------
;; complete-job
;; ---------------------------------------------------------------------------

(deftest complete-job-test
  (testing "marks job :completed and sets finalized-at"
    (let [j (drip/insert-job *client* "k2" {} nil)
          _ (drip/fetch-jobs *client* "default" "w1" :limit 1)
          done (drip/with-tx [tx *client*]
                 (drip-client/complete-job! *client* tx (:id j)))]
      (is (= :completed (:state done)))
      (is (some? (:finalized-at done)))))

  (testing "no-tx complete-job variant"
    (let [j (drip/insert-job *client* "k3" {} nil)
          _ (drip/fetch-jobs *client* "default" "w1" :limit 1)
          done (drip/complete-job *client* (:id j))]
      (is (= :completed (:state done)))
      (is (some? (:finalized-at done)))))

  (testing "completed job not re-fetched"
    (let [jobs (drip/fetch-jobs *client* "default" "w1" :limit 10)]
      (is (every? #(not= :completed (:state %)) jobs)))))

;; ---------------------------------------------------------------------------
;; fail-job
;; ---------------------------------------------------------------------------

(deftest fail-job-retryable
  (testing "failure with attempts remaining → :retryable"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 3})
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          failed (drip/with-tx [tx *client*]
                   (drip-client/fail-job! *client* tx (:id j)
                                          {:error "boom" :trace "stack"}
                                          job/default-retry-policy))]
      (is (= :retryable (:state failed)))
      (is (nil? (:finalized-at failed)))
      (is (= 1 (count (:errors failed))))
      (is (= "boom" (:error (first (:errors failed)))))
      (is (string? (:at (first (:errors failed)))))
      (is (.isAfter ^Instant (:scheduled-at failed) (:created-at failed))))))

(deftest fail-job-discarded
  (testing "failure at max-attempts → :discarded with finalized-at"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 1})
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          failed (drip/with-tx [tx *client*]
                   (drip-client/fail-job! *client* tx (:id j)
                                          {:error "fatal"}
                                          job/default-retry-policy))]
      (is (= :discarded (:state failed)))
      (is (some? (:finalized-at failed))))))

;; ---------------------------------------------------------------------------
;; cancel-job
;; ---------------------------------------------------------------------------

(deftest cancel-job-test
  (testing "cancels available job"
    (let [j (drip/insert-job *client* "k" {} nil)
          c (drip/cancel-job *client* (:id j))]
      (is (= :cancelled (:state c)))
      (is (some? (:finalized-at c)))))

  (testing "cancelling already-cancelled job is idempotent"
    (let [j (drip/insert-job *client* "k" {} nil)
          _ (drip/cancel-job *client* (:id j))
          c2 (drip/cancel-job *client* (:id j))]
      (is (= :cancelled (:state c2)))))

  (testing "cannot cancel completed job"
    (let [j (drip/insert-job *client* "k" {} nil)
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j)))
          after (drip/cancel-job *client* (:id j))]
      (is (= :completed (:state after))))))

;; ---------------------------------------------------------------------------
;; retry-job
;; ---------------------------------------------------------------------------

(deftest retry-job-test
  (testing "re-queues discarded job"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 1})
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/fail-job! *client* tx (:id j)
                                     {:error "x"} job/default-retry-policy))
          r (drip/retry-job *client* (:id j))]
      (is (= :available (:state r)))))

  (testing "re-queues cancelled job"
    (let [j (drip/insert-job *client* "k" {} nil)
          _ (drip/cancel-job *client* (:id j))
          r (drip/retry-job *client* (:id j))]
      (is (= :available (:state r)))))

  (testing "clears finalized-at when re-queuing a cancelled job"
    (let [j (drip/insert-job *client* "k" {} nil)
          cancelled (drip/cancel-job *client* (:id j))]
      (is (some? (:finalized-at cancelled)))
      (let [r (drip/retry-job *client* (:id j))]
        (is (= :available (:state r)))
        (is (nil? (:finalized-at r))))))

  (testing "clears finalized-at when re-queuing a discarded job"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 1 :queue "retry-discard-q"})
          _ (drip/fetch-jobs *client* "retry-discard-q" "w2" :limit 1)
          discarded (drip/with-tx [tx *client*]
                      (drip-client/fail-job! *client* tx (:id j)
                                             {:error "x"} job/default-retry-policy))]
      (is (= :discarded (:state discarded)))
      (is (some? (:finalized-at discarded)))
      (let [r (drip/retry-job *client* (:id j))]
        (is (= :available (:state r)))
        (is (nil? (:finalized-at r)))))))

;; ---------------------------------------------------------------------------
;; discard-job
;; ---------------------------------------------------------------------------

(deftest discard-job-test
  (testing "moves running job to :discarded"
    (let [j (drip/insert-job *client* "k" {} nil)
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          d (drip/discard-job *client* (:id j))]
      (is (= :discarded (:state d)))
      (is (some? (:finalized-at d)))))

  (testing "discard idempotent on already-discarded"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 1})
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/fail-job! *client* tx (:id j)
                                     {:error "x"} job/default-retry-policy))
          d (drip/discard-job *client* (:id j))]
      (is (= :discarded (:state d))))))

;; ---------------------------------------------------------------------------
;; promote-scheduled-jobs
;; ---------------------------------------------------------------------------

(deftest promote-scheduled-jobs-test
  (testing "promotes overdue scheduled job to :available"
    (let [past (.minusSeconds (Instant/now) 10)
          j (drip/insert-job *client* "k" {}
                             {:scheduled-at (.plusSeconds (Instant/now) 3600)})]
      (drip/with-tx [tx *client*]
        (jdbc/execute-one! tx
                           ["UPDATE drip_job SET state = 'scheduled', scheduled_at = ? WHERE id = ?"
                            (db/instant->ts past) (:id j)]))
      (let [n (drip/with-tx [tx *client*]
                (drip-client/promote-scheduled-jobs! *client* tx))
            promoted (drip/get-job *client* (:id j))]
        (is (pos? n))
        (is (= :available (:state promoted)))))))

;; ---------------------------------------------------------------------------
;; Unique jobs
;; ---------------------------------------------------------------------------

(deftest unique-job-by-args
  (testing "second insert with same args conflicts"
    (let [unique-opts {:unique-opts
                       {:by-args? true
                        :by-period nil
                        :by-queue? false
                        :by-state job/default-unique-states}}
          j1 (drip/insert-job *client* "u_kind" {:x 1} unique-opts)]
      (is (some? (:unique-key j1)))
      (is (thrown? Exception
                   (drip/insert-job *client* "u_kind" {:x 1} unique-opts)))))

  (testing "different args do not conflict"
    (let [unique-opts {:unique-opts
                       {:by-args? true
                        :by-period nil
                        :by-queue? false
                        :by-state job/default-unique-states}}]
      (drip/insert-job *client* "u_kind2" {:x 1} unique-opts)
      (is (some? (drip/insert-job *client* "u_kind2" {:x 2} unique-opts))))))

(deftest unique-job-by-queue
  (testing "same kind+args conflict per queue when by-queue? true"
    (let [opts {:unique-opts
                {:by-args? false
                 :by-period nil
                 :by-queue? true
                 :by-state job/default-unique-states}}]
      (drip/insert-job *client* "qk" {} (assoc opts :queue "qa"))
      (is (thrown? Exception
                   (drip/insert-job *client* "qk" {} (assoc opts :queue "qa"))))
      (is (some? (drip/insert-job *client* "qk" {} (assoc opts :queue "qb")))))))

;; ---------------------------------------------------------------------------
;; insert-many
;; ---------------------------------------------------------------------------

(deftest insert-many-test
  (testing "inserts all jobs transactionally"
    (let [jobs (drip/insert-many *client*
                                 [["km1" {:a 1} nil]
                                  ["km2" {:b 2} nil]
                                  ["km3" {:c 3} {:queue "bulk"}]])]
      (is (= 3 (count jobs)))
      (is (= #{"km1" "km2" "km3"} (into #{} (map :kind) jobs)))
      (is (some? (:id (first jobs))))))

  (testing "all-or-nothing: exception rolls back all inserts"
    (let [unique-opts {:unique-opts
                       {:by-args? true :by-period nil
                        :by-queue? false :by-state job/default-unique-states}}]
      (drip/insert-job *client* "tx_test" {:x 99} unique-opts)
      (let [before-count (count (drip/list-jobs *client* {:kind "tx_test_new"}))]
        (is (thrown? Exception
                     (drip/insert-many *client*
                                       [["tx_test_new" {:z 1} nil]
                                        ["tx_test" {:x 99} unique-opts]])))
        (is (= before-count (count (drip/list-jobs *client* {:kind "tx_test_new"}))))))))

;; ---------------------------------------------------------------------------
;; Transactional insert
;; ---------------------------------------------------------------------------

(deftest transactional-insert-test
  (testing "job visible after commit"
    (let [id-atom (atom nil)]
      (drip/with-tx [tx *client*]
        (let [j (drip/insert-job! *client* tx "tx_kind" {:v 1} nil)]
          (reset! id-atom (:id j))))
      (let [j (drip/get-job *client* @id-atom)]
        (is (= "tx_kind" (:kind j)))))))

;; ---------------------------------------------------------------------------
;; Explicit-tx variants
;; ---------------------------------------------------------------------------

(deftest explicit-tx-variants
  (testing "insert-many! with explicit tx"
    (let [jobs (drip/with-tx [tx *client*]
                 (drip/insert-many! *client* tx
                                    [["etx1" {:a 1} nil]
                                     ["etx2" {:b 2} nil]]))]
      (is (= 2 (count jobs)))
      (is (= #{"etx1" "etx2"} (into #{} (map :kind) jobs)))))

  (testing "get-job! with explicit tx"
    (let [j (drip/insert-job *client* "k" {} nil)
          fetched (drip/with-tx [tx *client*]
                    (drip/get-job! *client* tx (:id j)))]
      (is (= (:id j) (:id fetched)))))

  (testing "list-jobs! with explicit tx"
    (drip/insert-job *client* "etx_list" {} nil)
    (let [jobs (drip/with-tx [tx *client*]
                 (drip/list-jobs! *client* tx {:kind "etx_list"}))]
      (is (= 1 (count jobs)))))

  (testing "fetch-jobs! with explicit tx"
    (drip/insert-job *client* "etx_fetch" {} nil)
    (let [jobs (drip/with-tx [tx *client*]
                 (drip/fetch-jobs! *client* tx "default" "w" {:limit 10}))]
      (is (some #(= "etx_fetch" (:kind %)) jobs))))

  (testing "cancel-job! with explicit tx"
    (let [j (drip/insert-job *client* "k" {} nil)
          c (drip/with-tx [tx *client*]
              (drip/cancel-job! *client* tx (:id j)))]
      (is (= :cancelled (:state c)))))

  (testing "retry-job! with explicit tx"
    (let [j (drip/insert-job *client* "k" {} nil)
          _ (drip/cancel-job *client* (:id j))
          r (drip/with-tx [tx *client*]
              (drip/retry-job! *client* tx (:id j)))]
      (is (= :available (:state r)))))

  (testing "discard-job! with explicit tx"
    (let [j (drip/insert-job *client* "k" {} nil)
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          d (drip/with-tx [tx *client*]
              (drip/discard-job! *client* tx (:id j)))]
      (is (= :discarded (:state d))))))

;; ---------------------------------------------------------------------------
;; Custom retry policy
;; ---------------------------------------------------------------------------

(deftest custom-retry-policy-test
  (testing "custom retry policy controls next scheduled_at"
    (let [fixed-delay 60
          custom-policy (fn [_attempt] (.plusSeconds (Instant/now) fixed-delay))
          j (drip/insert-job *client* "k" {} {:max-attempts 3})
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          before (.plusSeconds (Instant/now) (- fixed-delay 5))
          failed (drip/with-tx [tx *client*]
                   (drip-client/fail-job! *client* tx (:id j)
                                          {:error "x"} custom-policy))]
      (is (= :retryable (:state failed)))
      (is (.isAfter ^Instant (:scheduled-at failed) before)))))

;; ---------------------------------------------------------------------------
;; promote retryable
;; ---------------------------------------------------------------------------

(deftest promote-retryable-jobs-test
  (testing "promotes overdue retryable job to :available"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 3})]
      (drip/with-tx [tx *client*]
        (jdbc/execute-one! tx
                           ["UPDATE drip_job SET state = 'retryable', scheduled_at = ? WHERE id = ?"
                            (db/instant->ts (.minusSeconds (Instant/now) 10)) (:id j)]))
      (drip/with-tx [tx *client*]
        (drip-client/promote-scheduled-jobs! *client* tx))
      (is (= :available (:state (drip/get-job *client* (:id j))))))))

;; ---------------------------------------------------------------------------
;; fail accumulates errors
;; ---------------------------------------------------------------------------

(deftest fail-job-accumulates-errors
  (testing "each failure appends a new error entry"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 5})]
      (drip/fetch-jobs *client* "default" "w" :limit 1)
      (drip/with-tx [tx *client*]
        (drip-client/fail-job! *client* tx (:id j)
                               {:error "first"} job/default-retry-policy))
      (drip/with-tx [tx *client*]
        (jdbc/execute-one! tx
                           ["UPDATE drip_job SET scheduled_at = ? WHERE id = ?"
                            (db/instant->ts (.minusSeconds (Instant/now) 1)) (:id j)]))
      (drip/with-tx [tx *client*]
        (drip-client/promote-scheduled-jobs! *client* tx))
      (drip/fetch-jobs *client* "default" "w" :limit 1)
      (drip/with-tx [tx *client*]
        (drip-client/fail-job! *client* tx (:id j)
                               {:error "second"} job/default-retry-policy))
      (let [job (drip/get-job *client* (:id j))]
        (is (= 2 (count (:errors job))))
        (is (= "first" (:error (first (:errors job)))))
        (is (= "second" (:error (second (:errors job)))))))))

;; ---------------------------------------------------------------------------
;; retry from retryable
;; ---------------------------------------------------------------------------

(deftest retry-job-from-retryable
  (testing "re-queues retryable job"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 3})
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/fail-job! *client* tx (:id j)
                                     {:error "x"} job/default-retry-policy))
          r (drip/retry-job *client* (:id j))]
      (is (= :available (:state r))))))

;; ---------------------------------------------------------------------------
;; attempted-by accumulates
;; ---------------------------------------------------------------------------

(deftest attempted-by-accumulates
  (testing "each fetch cycle appends worker-id to attempted-by"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 5})]
      (drip/fetch-jobs *client* "default" "w1" :limit 1)
      (drip/with-tx [tx *client*]
        (drip-client/fail-job! *client* tx (:id j) {:error "e1"} job/default-retry-policy))
      (drip/with-tx [tx *client*]
        (jdbc/execute-one! tx ["UPDATE drip_job SET scheduled_at = ? WHERE id = ?"
                               (db/instant->ts (.minusSeconds (Instant/now) 1)) (:id j)]))
      (drip/with-tx [tx *client*]
        (drip-client/promote-scheduled-jobs! *client* tx))
      (drip/fetch-jobs *client* "default" "w2" :limit 1)
      (let [job (drip/get-job *client* (:id j))]
        (is (= 2 (:attempt job)))
        (is (= ["w1" "w2"] (:attempted-by job)))))))

;; ---------------------------------------------------------------------------
;; list-jobs ORDER BY id DESC
;; ---------------------------------------------------------------------------

(deftest list-jobs-ordered-desc
  (testing "list-jobs returns jobs in descending id order"
    (let [j1 (drip/insert-job *client* "ord_test" {:n 1} nil)
          j2 (drip/insert-job *client* "ord_test" {:n 2} nil)
          j3 (drip/insert-job *client* "ord_test" {:n 3} nil)
          jobs (drip/list-jobs *client* {:kind "ord_test"})]
      (is (= [(:id j3) (:id j2) (:id j1)]
             (mapv :id jobs))))))

;; ---------------------------------------------------------------------------
;; Pagination
;; ---------------------------------------------------------------------------

(deftest list-jobs-pagination-test
  (testing "cursor pagination returns correct pages in descending id order"
    (let [j1 (drip/insert-job *client* "page_test" {:n 1} nil)
          j2 (drip/insert-job *client* "page_test" {:n 2} nil)
          j3 (drip/insert-job *client* "page_test" {:n 3} nil)
          j4 (drip/insert-job *client* "page_test" {:n 4} nil)
          j5 (drip/insert-job *client* "page_test" {:n 5} nil)
          page1 (drip/list-jobs *client* {:kind "page_test" :limit 2})
          cursor1 (:id (last page1))
          page2 (drip/list-jobs *client* {:kind "page_test" :limit 2 :after cursor1})
          cursor2 (:id (last page2))
          page3 (drip/list-jobs *client* {:kind "page_test" :limit 2 :after cursor2})]
      (is (= [(:id j5) (:id j4)] (mapv :id page1)))
      (is (= [(:id j3) (:id j2)] (mapv :id page2)))
      (is (= [(:id j1)] (mapv :id page3)))))

  (testing "empty result when cursor is past all records"
    (let [j (drip/insert-job *client* "page_empty" {} nil)
          page (drip/list-jobs *client* {:kind "page_empty" :after (:id j)})]
      (is (empty? page))))

  (testing ":after combined with :state filter"
    (let [j1 (drip/insert-job *client* "page_state" {} nil)
          j2 (drip/insert-job *client* "page_state" {} nil)
          j3 (drip/insert-job *client* "page_state" {} nil)
          page1 (drip/list-jobs *client* {:kind "page_state" :state :available :limit 2})
          page2 (drip/list-jobs *client* {:kind "page_state" :state :available
                                          :limit 2 :after (:id (last page1))})]
      (is (= 2 (count page1)))
      (is (= [(:id j1)] (mapv :id page2))))))

;; ---------------------------------------------------------------------------
;; insert-many! rollback
;; ---------------------------------------------------------------------------

(deftest insert-many!-rollback-test
  (testing "exception in caller's tx rolls back all inserts"
    (let [unique-opts {:unique-opts
                       {:by-args? true :by-period nil
                        :by-queue? false :by-state job/default-unique-states}}
          _ (drip/insert-job *client* "rb_seed" {:x 99} unique-opts)
          before-count (count (drip/list-jobs *client* {:kind "rb_new"}))]
      (is (thrown? Exception
                   (drip/with-tx [tx *client*]
                     (drip/insert-many! *client* tx
                                        [["rb_new" {:z 1} nil]
                                         ["rb_seed" {:x 99} unique-opts]]))))
      (is (= before-count (count (drip/list-jobs *client* {:kind "rb_new"})))))))

;; ---------------------------------------------------------------------------
;; Priority ordering
;; ---------------------------------------------------------------------------

(deftest fetch-jobs-priority-order
  (testing "lower priority number fetched first (one at a time)"
    (drip/insert-job *client* "prio" {} {:queue "prio-q" :priority 4})
    (drip/insert-job *client* "prio" {} {:queue "prio-q" :priority 2})
    (drip/insert-job *client* "prio" {} {:queue "prio-q" :priority 1})
    (drip/insert-job *client* "prio" {} {:queue "prio-q" :priority 3})
    (let [p1 (:priority (first (drip/fetch-jobs *client* "prio-q" "w" :limit 1)))
          p2 (:priority (first (drip/fetch-jobs *client* "prio-q" "w" :limit 1)))
          p3 (:priority (first (drip/fetch-jobs *client* "prio-q" "w" :limit 1)))
          p4 (:priority (first (drip/fetch-jobs *client* "prio-q" "w" :limit 1)))]
      (is (= [1 2 3 4] [p1 p2 p3 p4])))))

;; ---------------------------------------------------------------------------
;; delete-jobs
;; ---------------------------------------------------------------------------

(deftest delete-jobs-by-state
  (testing "deletes completed jobs matching state filter"
    (let [j1 (drip/insert-job *client* "del_test" {} nil)
          j2 (drip/insert-job *client* "del_test" {} nil)
          _ (drip/fetch-jobs *client* "default" "w" :limit 10)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j1))
              (drip-client/complete-job! *client* tx (:id j2)))
          n (drip/delete-jobs *client* {:states [:completed]})]
      (is (>= n 2))
      (is (nil? (drip/get-job *client* (:id j1))))
      (is (nil? (drip/get-job *client* (:id j2)))))))

(deftest delete-jobs-finalized-before
  (testing "only deletes jobs whose finalized_at is before the cutoff"
    (let [j-old (drip/insert-job *client* "del_old" {} nil)
          j-new (drip/insert-job *client* "del_new" {} nil)
          _ (drip/fetch-jobs *client* "default" "w" :limit 10)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j-old))
              (drip-client/complete-job! *client* tx (:id j-new)))
          cutoff (.minusSeconds (Instant/now) 86400)
          _ (drip/with-tx [tx *client*]
              (jdbc/execute-one! tx
                                 ["UPDATE drip_job SET finalized_at = ? WHERE id = ?"
                                  (db/instant->ts (.minusSeconds (Instant/now) 172800)) (:id j-old)]))
          n (drip/delete-jobs *client* {:states [:completed] :finalized-before cutoff})]
      (is (>= n 1))
      (is (nil? (drip/get-job *client* (:id j-old))))
      (is (some? (drip/get-job *client* (:id j-new)))))))

(deftest delete-jobs-multiple-states
  (testing "deletes across multiple states in one call"
    (let [j-done (drip/insert-job *client* "multi_del" {} nil)
          j-cancel (drip/insert-job *client* "multi_del" {} nil)
          _ (drip/fetch-jobs *client* "default" "w" :limit 10)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j-done)))
          _ (drip/cancel-job *client* (:id j-cancel))
          n (drip/delete-jobs *client* {:states [:completed :cancelled]})]
      (is (>= n 2))
      (is (nil? (drip/get-job *client* (:id j-done))))
      (is (nil? (drip/get-job *client* (:id j-cancel)))))))

;; ---------------------------------------------------------------------------
;; delete-job single
;; ---------------------------------------------------------------------------

(deftest delete-job-test
  (testing "returns deleted job and removes it"
    (let [j (drip/insert-job *client* "del_single" {} nil)
          deleted (drip/delete-job *client* (:id j))]
      (is (= (:id j) (:id deleted)))
      (is (nil? (drip/get-job *client* (:id j))))))
  (testing "returns nil when job does not exist"
    (is (nil? (drip/delete-job *client* -1)))))

;; ---------------------------------------------------------------------------
;; update-job
;; ---------------------------------------------------------------------------

(deftest update-job-test
  (testing "updates metadata"
    (let [j (drip/insert-job *client* "k" {} nil)
          updated (drip/update-job *client* (:id j) {:metadata {:foo "bar"}})]
      (is (= {"foo" "bar"} (:metadata updated)))))

  (testing "updates priority"
    (let [j (drip/insert-job *client* "k" {} nil)
          updated (drip/update-job *client* (:id j) {:priority 3})]
      (is (= 3 (:priority updated)))))

  (testing "updates queue"
    (let [j (drip/insert-job *client* "k" {} nil)
          updated (drip/update-job *client* (:id j) {:queue "other"})]
      (is (= "other" (:queue updated)))))

  (testing "updates max-attempts"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 5})
          updated (drip/update-job *client* (:id j) {:max-attempts 10})]
      (is (= 10 (:max-attempts updated)))))

  (testing "updates tags"
    (let [j (drip/insert-job *client* "k" {} nil)
          updated (drip/update-job *client* (:id j) {:tags ["a" "b"]})]
      (is (= ["a" "b"] (:tags updated)))))

  (testing "scheduled-at in future sets state :scheduled"
    (let [j (drip/insert-job *client* "k" {} nil)
          future-ts (.plusSeconds (Instant/now) 3600)
          updated (drip/update-job *client* (:id j) {:scheduled-at future-ts})]
      (is (= :scheduled (:state updated)))
      (is (.isAfter ^Instant (:scheduled-at updated) (Instant/now)))))

  (testing "scheduled-at in past sets state :available"
    (let [j (drip/insert-job *client* "k" {} {:scheduled-at (.plusSeconds (Instant/now) 3600)})
          past-ts (.minusSeconds (Instant/now) 10)
          updated (drip/update-job *client* (:id j) {:scheduled-at past-ts})]
      (is (= :available (:state updated)))))

  (testing "empty opts returns job unchanged"
    (let [j (drip/insert-job *client* "k" {} {:priority 2})
          updated (drip/update-job *client* (:id j) {})]
      (is (= 2 (:priority updated)))))

  (testing "returns nil for unknown job id"
    (is (nil? (drip/update-job *client* -1 {:priority 2}))))

  (testing "update-job! with explicit tx"
    (let [j (drip/insert-job *client* "k" {} nil)
          updated (drip/with-tx [tx *client*]
                    (drip/update-job! *client* tx (:id j) {:metadata {:via "tx"}}))]
      (is (= {"via" "tx"} (:metadata updated))))))

;; ---------------------------------------------------------------------------
;; swap-job
;; ---------------------------------------------------------------------------

(deftest swap-job-test
  (testing "applies f and updates returned fields"
    (let [j (drip/insert-job *client* "k" {} {:priority 2})
          result (drip/swap-job *client* (:id j)
                                (fn [job] {:priority (inc (:priority job))}))]
      (is (= 3 (:priority result)))))

  (testing "f receives current job state"
    (let [j (drip/insert-job *client* "k" {} {:metadata {:x 1}})
          result (drip/swap-job *client* (:id j)
                                (fn [job] {:metadata (update (:metadata job) "x" inc)}))]
      (is (= {"x" 2} (:metadata result)))))

  (testing "multiple fields updated atomically"
    (let [j (drip/insert-job *client* "k" {} {:priority 1})
          result (drip/swap-job *client* (:id j)
                                (fn [_job] {:priority 4 :queue "other" :tags ["swapped"]}))]
      (is (= 4 (:priority result)))
      (is (= "other" (:queue result)))
      (is (= ["swapped"] (:tags result)))))

  (testing "returns nil for unknown job id"
    (is (nil? (drip/swap-job *client* -1 (fn [_] {:priority 2})))))

  (testing "swap-job! with explicit tx"
    (let [j (drip/insert-job *client* "k" {} {:priority 1})
          result (drip/with-tx [tx *client*]
                   (drip/swap-job! *client* tx (:id j)
                                   (fn [job] {:priority (+ 2 (:priority job))})))]
      (is (= 3 (:priority result)))))

  (testing "persisted — get-job reflects swap"
    (let [j (drip/insert-job *client* "k" {} {:priority 1})
          _ (drip/swap-job *client* (:id j) (fn [_] {:queue "swapped-q"}))
          fetched (drip/get-job *client* (:id j))]
      (is (= "swapped-q" (:queue fetched))))))

;; ---------------------------------------------------------------------------
;; snooze-job
;; ---------------------------------------------------------------------------

(deftest snooze-job-test
  (testing "rescheduled job has state :scheduled and attempt count unchanged"
    (let [j (drip/insert-job *client* "snooze_kind" {} {:queue "snooze-q"})
          _ (drip/fetch-jobs *client* "snooze-q" "w" :limit 1)
          running (drip/get-job *client* (:id j))
          _ (is (= :running (:state running)))
          attempt-before (:attempt running)
          snoozed (drip/with-tx [tx *client*]
                    (drip/snooze-job! *client* tx (:id j) "60s"))]
      (is (= :scheduled (:state snoozed)))
      (is (= attempt-before (:attempt snoozed)))
      (is (.isAfter ^Instant (:scheduled-at snoozed) (Instant/now)))))
  (testing "snooze-job (no-tx variant) works"
    (let [j (drip/insert-job *client* "snooze_kind2" {} {:queue "snooze-q2"})
          _ (drip/fetch-jobs *client* "snooze-q2" "w" :limit 1)
          snoozed (drip/snooze-job *client* (:id j) "30s")]
      (is (= :scheduled (:state snoozed))))))

;; ---------------------------------------------------------------------------
;; insert-many-fast! (PostgreSQL only)
;; ---------------------------------------------------------------------------

(deftest insert-many-fast-test
  (testing "inserts jobs via COPY and they are fetchable"
    (when (= "s_exp.drip.client.postgres.PostgresClient" (.getName (class *client*)))
      (let [n 100
            job-specs (mapv (fn [i] ["fast_kind" {:i i} {:queue "default"}]) (range n))
            inserted (with-open [conn (.getConnection ^javax.sql.DataSource (:ds *client*))]
                       (drip/insert-many-fast! conn job-specs))
            listed (drip/list-jobs *client* {:kind "fast_kind" :limit (+ n 10)})]
        (is (= n inserted))
        (is (= n (count listed)))
        (is (every? #(= :available (:state %)) listed))))))
