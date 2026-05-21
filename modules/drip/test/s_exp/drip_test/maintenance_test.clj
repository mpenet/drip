(ns s-exp.drip-test.maintenance-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc :as jdbc]
            [s-exp.drip :as drip]
            [s-exp.drip-test.fixture :refer [*client* with-db]]
            [s-exp.drip.client :as drip-client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.drip.maintenance :as maintenance])
  (:import (java.time Instant)))

(use-fixtures :each with-db)

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- postgres? []
  (= "s_exp.drip.client.postgres.PostgresClient" (.getName (class *client*))))

(defn- backdate-finalized! [job-id seconds-ago]
  (drip/with-tx [tx *client*]
    (jdbc/execute-one! tx
                       ["UPDATE drip_job SET finalized_at = ? WHERE id = ?"
                        (db/instant->str (.minusSeconds (Instant/now) seconds-ago))
                        job-id])))

(defn- backdate-attempted! [job-id seconds-ago]
  (drip/with-tx [tx *client*]
    (jdbc/execute-one! tx
                       ["UPDATE drip_job SET attempted_at = ? WHERE id = ?"
                        (db/instant->str (.minusSeconds (Instant/now) seconds-ago))
                        job-id])))

;; ---------------------------------------------------------------------------
;; run-cleaner (retention)
;; ---------------------------------------------------------------------------

(deftest retention-deletes-old-completed
  (testing "completed job older than threshold is deleted"
    (let [j (drip/insert-job *client* "ret_done" {} {:queue "ret-q"})
          _ (drip/fetch-jobs *client* "ret-q" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j)))
          _ (backdate-finalized! (:id j) (* 25 3600))]
      (drip/with-tx [tx *client*]
        (#'maintenance/run-cleaner *client* tx {:default {:completed "24h"}} ["ret-q"]))
      (is (nil? (drip/get-job *client* (:id j)))))))

(deftest retention-keeps-recent-completed
  (testing "completed job within threshold is not deleted"
    (let [j (drip/insert-job *client* "ret_recent" {} {:queue "ret-recent-q"})
          _ (drip/fetch-jobs *client* "ret-recent-q" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j)))]
      (drip/with-tx [tx *client*]
        (#'maintenance/run-cleaner *client* tx {:default {:completed "24h"}} ["ret-recent-q"]))
      (is (some? (drip/get-job *client* (:id j)))))))

(deftest retention-nil-state-skips-that-state
  (testing "nil value for a state key disables deletion for that state"
    (let [j (drip/insert-job *client* "ret_nil" {} {:queue "ret-nil-q"})
          _ (drip/fetch-jobs *client* "ret-nil-q" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j)))
          _ (backdate-finalized! (:id j) (* 25 3600))]
      (drip/with-tx [tx *client*]
        (#'maintenance/run-cleaner *client* tx {:default {:completed nil}} ["ret-nil-q"]))
      (is (some? (drip/get-job *client* (:id j)))))))

(deftest retention-multiple-states
  (testing "deletes completed and cancelled independently"
    (let [j-done (drip/insert-job *client* "ret_multi" {} {:queue "ret-multi-q"})
          j-cancel (drip/insert-job *client* "ret_multi" {} {:queue "ret-multi-q"})
          _ (drip/fetch-jobs *client* "ret-multi-q" "w" :limit 10)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j-done)))
          _ (drip/cancel-job *client* (:id j-cancel))
          _ (backdate-finalized! (:id j-done) (* 25 3600))
          _ (backdate-finalized! (:id j-cancel) (* 25 3600))]
      (drip/with-tx [tx *client*]
        (#'maintenance/run-cleaner *client* tx
                                   {:default {:completed "24h" :cancelled "24h"}}
                                   ["ret-multi-q"]))
      (is (nil? (drip/get-job *client* (:id j-done))))
      (is (nil? (drip/get-job *client* (:id j-cancel)))))))

(deftest retention-per-queue-override
  (testing "per-queue retention overrides global for that queue"
    (let [j-long (drip/insert-job *client* "ret_pq" {} {:queue "ret-long-q"})
          j-short (drip/insert-job *client* "ret_pq" {} {:queue "ret-short-q"})
          _ (drip/fetch-jobs *client* "ret-long-q" "w" :limit 1)
          _ (drip/fetch-jobs *client* "ret-short-q" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j-long))
              (drip-client/complete-job! *client* tx (:id j-short)))
          _ (backdate-finalized! (:id j-long) (* 25 3600))
          _ (backdate-finalized! (:id j-short) (* 25 3600))]
      ;; ret-long-q overrides completed retention to 48h — job is only 25h old, survives
      ;; ret-short-q uses default 24h — job is 25h old, deleted
      (drip/with-tx [tx *client*]
        (#'maintenance/run-cleaner *client* tx
                                   {:default {:completed "24h"}
                                    "ret-long-q" {:completed "48h"}}
                                   ["ret-long-q" "ret-short-q"]))
      (is (some? (drip/get-job *client* (:id j-long))))
      (is (nil? (drip/get-job *client* (:id j-short)))))))

;; ---------------------------------------------------------------------------
;; run-rescue
;; ---------------------------------------------------------------------------

(deftest rescue-retryable
  (testing "stuck running job with attempts remaining → :retryable"
    (let [j (drip/insert-job *client* "rr_retryable" {} {:max-attempts 3 :queue "rr-retry-q"})
          _ (drip/fetch-jobs *client* "rr-retry-q" "w" :limit 1)
          _ (backdate-attempted! (:id j) 7200)]
      (drip/with-tx [tx *client*]
        (#'maintenance/run-rescue *client* tx
                                  {:default "1h"}
                                  job/default-retry-policy
                                  ["rr-retry-q"]))
      (is (= :retryable (:state (drip/get-job *client* (:id j))))))))

(deftest rescue-discards-exhausted
  (testing "stuck running job with no attempts remaining → :discarded"
    (let [j (drip/insert-job *client* "rr_discard" {} {:max-attempts 1 :queue "rr-discard-q"})
          _ (drip/fetch-jobs *client* "rr-discard-q" "w" :limit 1)
          _ (backdate-attempted! (:id j) 7200)]
      (drip/with-tx [tx *client*]
        (#'maintenance/run-rescue *client* tx
                                  {:default "1h"}
                                  job/default-retry-policy
                                  ["rr-discard-q"]))
      (is (= :discarded (:state (drip/get-job *client* (:id j)))))
      (is (some? (:finalized-at (drip/get-job *client* (:id j))))))))

(deftest rescue-respects-threshold
  (testing "recently stuck job is not rescued"
    (let [j (drip/insert-job *client* "rr_recent" {} {:queue "rr-recent-q"})
          _ (drip/fetch-jobs *client* "rr-recent-q" "w" :limit 1)]
      (drip/with-tx [tx *client*]
        (#'maintenance/run-rescue *client* tx
                                  {:default "1h"}
                                  job/default-retry-policy
                                  ["rr-recent-q"]))
      (is (= :running (:state (drip/get-job *client* (:id j))))))))

(deftest rescue-per-queue-threshold
  (testing "per-queue rescue-after overrides default for that queue"
    (let [j-fast (drip/insert-job *client* "rr_pq" {} {:max-attempts 3 :queue "rr-fast-q"})
          j-slow (drip/insert-job *client* "rr_pq" {} {:max-attempts 3 :queue "rr-slow-q"})
          _ (drip/fetch-jobs *client* "rr-fast-q" "w" :limit 1)
          _ (drip/fetch-jobs *client* "rr-slow-q" "w" :limit 1)
          ;; both stuck for 30 minutes
          _ (backdate-attempted! (:id j-fast) 1800)
          _ (backdate-attempted! (:id j-slow) 1800)]
      ;; rr-fast-q threshold is 15m — rescues; rr-slow-q uses default 1h — does not rescue
      (drip/with-tx [tx *client*]
        (#'maintenance/run-rescue *client* tx
                                  {:default "1h" "rr-fast-q" "15m"}
                                  job/default-retry-policy
                                  ["rr-fast-q" "rr-slow-q"]))
      (is (= :retryable (:state (drip/get-job *client* (:id j-fast)))))
      (is (= :running (:state (drip/get-job *client* (:id j-slow))))))))

(deftest rescue-nil-default-disabled
  (testing "nil :default disables global rescue when no per-queue override"
    (let [j (drip/insert-job *client* "rr_nil" {} {:max-attempts 3 :queue "rr-nil-q"})
          _ (drip/fetch-jobs *client* "rr-nil-q" "w" :limit 1)
          _ (backdate-attempted! (:id j) 7200)]
      (drip/with-tx [tx *client*]
        (#'maintenance/run-rescue *client* tx
                                  {:default nil}
                                  job/default-retry-policy
                                  ["rr-nil-q"]))
      (is (= :running (:state (drip/get-job *client* (:id j))))))))

;; ---------------------------------------------------------------------------
;; Maintenance worker lifecycle
;; ---------------------------------------------------------------------------

(deftest maintenance-worker-lifecycle
  (testing "start/stop returns clean shutdown"
    (let [mw (drip/start-maintenance-worker!
              {:client *client*
               :rescue-interval 999999
               :retention-interval 999999})]
      (is (true? (drip/stop-maintenance-worker! mw :timeout "5s")))))

  (testing "disabled rescue — rescue-scheduler is nil"
    (let [mw (maintenance/start-maintenance-worker!
              {:client *client*
               :rescue-after nil
               :retention nil})]
      (is (nil? (:rescue-scheduler mw)))
      (is (nil? (:retention-scheduler mw)))
      (maintenance/stop-maintenance-worker! mw :timeout "1s")))

  (testing "disabled reindex — reindex-scheduler is nil"
    (let [mw (maintenance/start-maintenance-worker!
              {:client *client*
               :rescue-interval 999999
               :retention-interval 999999
               :reindex-interval nil})]
      (is (nil? (:reindex-scheduler mw)))
      (maintenance/stop-maintenance-worker! mw :timeout "1s")))

  (testing "reindex-interval set — reindex-scheduler is non-nil"
    (let [mw (maintenance/start-maintenance-worker!
              {:client *client*
               :rescue-interval 999999
               :retention-interval 999999
               :reindex-interval 999999})]
      (is (some? (:reindex-scheduler mw)))
      (maintenance/stop-maintenance-worker! mw :timeout "1s"))))

(deftest maintenance-worker-retention-integration
  (testing "maintenance worker deletes old completed jobs"
    (let [j (drip/insert-job *client* "mw_ret" {} {:queue "mw-ret-q"})
          _ (drip/fetch-jobs *client* "mw-ret-q" "w" :limit 1)
          _ (drip/with-tx [tx *client*]
              (drip-client/complete-job! *client* tx (:id j)))
          _ (backdate-finalized! (:id j) (* 25 3600))
          mw (drip/start-maintenance-worker!
              {:client *client*
               :queues ["mw-ret-q"]
               :rescue-after nil
               :retention {:default {:completed "24h"}}
               :retention-interval 50})]
      (try
        (Thread/sleep 300)
        (drip/stop-maintenance-worker! mw :timeout "5s")
        (is (nil? (drip/get-job *client* (:id j))))
        (finally
          (try (drip/stop-maintenance-worker! mw :timeout "1s") (catch Exception _ nil)))))))

;; ---------------------------------------------------------------------------
;; reindex! (PostgreSQL only)
;; ---------------------------------------------------------------------------

(deftest reindex-returns-map
  (testing "reindex! returns a map of index-name → status"
    (let [result (drip/reindex! *client*)]
      (is (map? result))
      (if (postgres?)
        (do
          (is (pos? (count result)))
          (is (every? keyword? (keys result)))
          (is (every? #{:reindexed :skipped :not-found} (vals result))))
        (is (empty? result))))))
