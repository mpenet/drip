(ns s-exp.drip-test.worker-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc :as jdbc]
            [s-exp.drip :as drip]
            [s-exp.drip-test.fixture :refer [*client* with-db]]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.drip.worker :as worker])
  (:import (java.time Instant)))

(use-fixtures :each with-db)

;; ---------------------------------------------------------------------------
;; Worker / Executor — end-to-end
;; ---------------------------------------------------------------------------

(deftest worker-processes-job
  (testing "executor picks up and completes a job"
    (let [results (atom [])
          registry {"work_item" (fn [client job]
                                  (swap! results conj (:args job))
                                  (drip/complete-job client (:id job)))}
          executor (worker/start-executor!
                    {:client *client*
                     :registry registry
                     :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "work_item" {:val 42} nil)
        (Thread/sleep 500)
        (is (= [{:val 42}] @results))
        (let [jobs (drip/list-jobs *client* {:kind "work_item" :state :completed})]
          (is (= 1 (count jobs))))
        (finally
          (worker/stop-executor! executor 5000))))))

(deftest worker-handles-failure
  (testing "failing worker records error and marks job :retryable (max-attempts > 1)"
    (let [processed (promise)
          registry {"fail_work" (fn [_ _job]
                                  (deliver processed true)
                                  (throw (RuntimeException. "intentional")))}
          executor (worker/start-executor!
                    {:client *client*
                     :registry registry
                     :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "fail_work" {} {:max-attempts 3})
        (deref processed 5000 nil)
        (worker/stop-executor! executor 5000)
        (let [job (first (drip/list-jobs *client* {:kind "fail_work"}))]
          (is (some? job))
          (is (contains? #{:retryable :available} (:state job)))
          (is (>= (count (:errors job)) 1))
          (is (= "intentional" (:error (first (:errors job))))))
        (finally
          (try (worker/stop-executor! executor 1000) (catch Exception _ nil)))))))

(deftest worker-discards-unknown-kind
  (testing "job with no matching worker is discarded"
    (let [registry {}
          executor (worker/start-executor!
                    {:client *client*
                     :registry registry
                     :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "unknown_kind" {} nil)
        (Thread/sleep 500)
        (let [jobs (drip/list-jobs *client* {:kind "unknown_kind" :state :discarded})]
          (is (= 1 (count jobs))))
        (finally
          (worker/stop-executor! executor 5000))))))

(deftest worker-concurrency-semaphore
  (testing "semaphore limits concurrency and all jobs eventually complete"
    (let [n-done (atom 0)
          done (promise)
          total 6
          registry {"conc_work" (fn [client job]
                                  (when (= total (swap! n-done inc))
                                    (deliver done true))
                                  (drip/complete-job client (:id job)))}
          executor (worker/start-executor!
                    {:client *client*
                     :registry registry
                     :concurrency 3
                     :poll-interval-ms 50})]
      (try
        (dotimes [_ total] (drip/insert-job *client* "conc_work" {} nil))
        (deref done 10000 nil)
        (worker/stop-executor! executor 5000)
        (let [jobs (drip/list-jobs *client* {:kind "conc_work" :state :completed})]
          (is (= total (count jobs))))
        (finally
          (try (worker/stop-executor! executor 1000) (catch Exception _ nil)))))))

(deftest worker-picks-up-pre-inserted-job
  (testing "job inserted before executor start is processed"
    (let [done (promise)
          _ (drip/insert-job *client* "pre_insert" {:v 1} nil)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"pre_insert" (fn [_ _job] (deliver done true))}
                     :poll-interval-ms 50})]
      (try
        (is (deref done 5000 false))
        (finally
          (worker/stop-executor! executor 5000))))))

(deftest worker-graceful-shutdown
  (testing "stop-executor! returns after in-flight jobs finish"
    (let [started (promise)
          finished (atom false)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"slow_job" (fn [_ _job]
                                             (deliver started true)
                                             (Thread/sleep 200)
                                             (reset! finished true))}
                     :poll-interval-ms 50})]
      (drip/insert-job *client* "slow_job" {} nil)
      (deref started 5000 nil)
      (worker/stop-executor! executor 5000)
      (is @finished))))

(deftest stop-and-cancel-test
  (testing "stop-and-cancel! interrupts in-flight jobs; job remains rescuable"
    (let [started (promise)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"cancel_job" (fn [_ _job]
                                               (deliver started true)
                                               (Thread/sleep 10000))}
                     :poll-interval-ms 50})]
      (drip/insert-job *client* "cancel_job" {} nil)
      (deref started 5000 nil)
      (drip/stop-and-cancel! executor)
      ;; job should still be :running (not completed/discarded) — rescue will handle it
      (let [job (first (drip/list-jobs *client* {:kind "cancel_job"}))]
        (is (some? job))
        (is (= :running (:state job)))))))

(deftest periodic-executor-test
  (testing "periodic spec inserts jobs at the given interval"
    (let [scheduler
          (drip/start-periodic-executor!
           *client*
           [{:kind "periodic_job"
             :args {:tick true}
             :period 100
             :queue "default"
             :opts nil}])]
      (try
        (Thread/sleep 350)
        (let [jobs (drip/list-jobs *client* {:kind "periodic_job"})]
          (is (>= (count jobs) 1)))
        (finally
          (drip/stop-periodic-executor! scheduler))))))

(deftest periodic-executor-dedup
  (testing "duplicate insertions within period conflict"
    (let [unique-opts {:unique-opts
                       {:by-args? false
                        :by-period "1h"
                        :by-queue? true
                        :by-state job/default-unique-states}}]
      (drip/insert-job *client* "dedup_test" {} (merge {:queue "default"} unique-opts))
      (is (thrown? Exception
                   (drip/insert-job *client* "dedup_test" {}
                                    (merge {:queue "default"} unique-opts)))))))

(deftest rescue-stuck-jobs-retryable
  (testing "stuck running job (attempts remaining) → :retryable with error appended"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 3})
          _ (drip/fetch-jobs *client* "default" "w" :limit 1)
          stuck-at (.minusSeconds (Instant/now) 7200)
          _ (drip/with-tx [tx *client*]
              (jdbc/execute-one! tx
                                 ["UPDATE drip_job SET attempted_at = ? WHERE id = ?"
                                  (db/instant->ts stuck-at) (:id j)]))
          n (drip/rescue-stuck-jobs *client* (.minusSeconds (Instant/now) 3600) job/default-retry-policy)
          rescued (drip/get-job *client* (:id j))]
      (is (= 1 n))
      (is (= :retryable (:state rescued)))
      (is (nil? (:finalized-at rescued)))
      (is (= 1 (count (:errors rescued))))
      (is (= "job rescued after timeout" (:error (first (:errors rescued))))))))

(deftest rescue-stuck-jobs-discarded
  (testing "stuck running job (exhausted attempts) → :discarded with finalized-at"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 1 :queue "rescue-discard-q"})
          _ (drip/fetch-jobs *client* "rescue-discard-q" "w" :limit 1)
          stuck-at (.minusSeconds (Instant/now) 7200)
          _ (drip/with-tx [tx *client*]
              (jdbc/execute-one! tx
                                 ["UPDATE drip_job SET attempted_at = ? WHERE id = ?"
                                  (db/instant->ts stuck-at) (:id j)]))
          n (drip/rescue-stuck-jobs *client* (.minusSeconds (Instant/now) 3600) job/default-retry-policy)
          rescued (drip/get-job *client* (:id j))]
      (is (pos? n))
      (is (= :discarded (:state rescued)))
      (is (some? (:finalized-at rescued))))))

(deftest rescue-stuck-jobs-not-old-enough
  (testing "recently stuck job (within threshold) is not rescued"
    (let [j (drip/insert-job *client* "k" {} {:queue "rescue-recent-q"})
          _ (drip/fetch-jobs *client* "rescue-recent-q" "w" :limit 1)
          n (drip/rescue-stuck-jobs *client* (.minusSeconds (Instant/now) 3600) job/default-retry-policy)
          still-running (drip/get-job *client* (:id j))]
      (is (= 0 n))
      (is (= :running (:state still-running))))))

(deftest rescue-stuck-jobs!-explicit-tx
  (testing "rescue-stuck-jobs! works with explicit tx"
    (let [j (drip/insert-job *client* "k" {} {:max-attempts 3 :queue "rescue-tx-q"})
          _ (drip/fetch-jobs *client* "rescue-tx-q" "w" :limit 1)
          stuck-at (.minusSeconds (Instant/now) 7200)
          _ (drip/with-tx [tx *client*]
              (jdbc/execute-one! tx
                                 ["UPDATE drip_job SET attempted_at = ? WHERE id = ?"
                                  (db/instant->ts stuck-at) (:id j)]))
          n (drip/with-tx [tx *client*]
              (drip/rescue-stuck-jobs! *client* tx (.minusSeconds (Instant/now) 3600) job/default-retry-policy))]
      (is (pos? n))
      (is (= :retryable (:state (drip/get-job *client* (:id j))))))))

(deftest rescue-stuck-jobs-queue-filter
  (testing "rescue-stuck-jobs with queues filter only rescues matching queue"
    (let [j-a (drip/insert-job *client* "k" {} {:max-attempts 3 :queue "rescue-queue-a"})
          j-b (drip/insert-job *client* "k" {} {:max-attempts 3 :queue "rescue-queue-b"})
          _ (drip/fetch-jobs *client* "rescue-queue-a" "w" :limit 1)
          _ (drip/fetch-jobs *client* "rescue-queue-b" "w" :limit 1)
          stuck-at (.minusSeconds (Instant/now) 7200)
          _ (drip/with-tx [tx *client*]
              (jdbc/execute-one! tx
                                 ["UPDATE drip_job SET attempted_at = ? WHERE id IN (?, ?)"
                                  (db/instant->ts stuck-at) (:id j-a) (:id j-b)]))]
      ;; rescue only queue-a
      (drip/rescue-stuck-jobs *client* (.minusSeconds (Instant/now) 3600)
                              job/default-retry-policy ["rescue-queue-a"])
      (is (= :retryable (:state (drip/get-job *client* (:id j-a)))))
      (is (= :running (:state (drip/get-job *client* (:id j-b))))))))

(deftest per-kind-retry-policy-test
  (testing ":retry-policies unified map is stored in executor record"
    (let [custom-policy (fn [_attempt] (.plusSeconds (Instant/now) 1))
          ex (drip/start-executor!
              {:client *client*
               :registry {}
               :retry-policies {:default drip/default-retry-policy
                                "my_kind" custom-policy}
               :poll-interval-ms 999999})]
      (is (= custom-policy (get (:retry-policies ex) "my_kind")))
      (is (= drip/default-retry-policy (get (:retry-policies ex) :default)))
      (drip/stop-executor! ex 1000))))

(deftest job-timeout-test
  (testing "timed-out job is failed with timeout error message"
    (let [started (promise)
          registry {"timeout_kind" (fn [_ _job]
                                     (deliver started true)
                                     (Thread/sleep 10000))}
          ex (worker/start-executor!
              {:client *client*
               :registry registry
               :job-timeouts {:default 200}
               :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "timeout_kind" {} nil)
        (deref started 5000 nil)
        (Thread/sleep 500)
        (worker/stop-executor! ex 5000)
        (let [job (first (drip/list-jobs *client* {:kind "timeout_kind"}))]
          (is (some? job))
          (is (contains? #{:retryable :discarded} (:state job)))
          (is (str/includes? (:error (first (:errors job))) "timed out")))
        (finally
          (try (worker/stop-executor! ex 1000) (catch Exception _ nil))))))
  (testing "per-kind timeout stored in executor record"
    (let [ex (drip/start-executor!
              {:client *client*
               :registry {}
               :job-timeouts {"slow_kind" 500}
               :poll-interval-ms 999999})]
      (is (= 500 (get (:job-timeouts ex) "slow_kind")))
      (drip/stop-executor! ex 1000))))

(deftest rescue-after-unified-map-test
  (testing ":rescue-after unified map stored in executor record"
    (let [ex (drip/start-executor!
              {:client *client*
               :registry {}
               :rescue-after {:default "2h" "slow" "4h"}
               :poll-interval-ms 999999})]
      (is (= "2h" (get (:rescue-after ex) :default)))
      (is (= "4h" (get (:rescue-after ex) "slow")))
      (drip/stop-executor! ex 1000))))

(deftest handler-receives-client-and-job
  (testing "handler receives client as first arg and job as second"
    (let [capture (promise)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"ctx_kind" (fn [client job]
                                             (deliver capture {:client client :job job}))}
                     :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "ctx_kind" {:x 1} nil)
        (let [{:keys [client job]} (deref capture 5000 nil)]
          (is (some? job))
          (is (= {:x 1} (:args job)))
          (is (= "ctx_kind" (:kind job)))
          (is (= *client* client)))
        (finally
          (worker/stop-executor! executor 5000))))))

(deftest explicit-complete-in-handler
  (testing "handler calling complete-job! directly marks job completed"
    (let [done (promise)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"explicit_complete_kind"
                                (fn [client job]
                                  (drip/with-tx [tx client]
                                    (drip/complete-job! client tx (:id job)))
                                  (deliver done true))}
                     :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "explicit_complete_kind" {} nil)
        (is (deref done 5000 false))
        (Thread/sleep 100)
        (worker/stop-executor! executor 5000)
        (let [jobs (drip/list-jobs *client* {:kind "explicit_complete_kind" :state :completed})]
          (is (= 1 (count jobs))))
        (finally
          (try (worker/stop-executor! executor 1000) (catch Exception _ nil)))))))

(deftest outbox-pattern-atomic-completion
  (testing "handler can complete job inside its own tx alongside business writes"
    (let [done (promise)
          side-effect (atom nil)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"outbox_kind"
                                (fn [client job]
                                  (drip/with-tx [tx client]
                                    (drip/update-job! client tx (:id job)
                                                      {:metadata {:processed true}})
                                    (drip/complete-job! client tx (:id job)))
                                  (reset! side-effect :done)
                                  (deliver done true))}
                     :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "outbox_kind" {:payload "data"} nil)
        (is (deref done 5000 false))
        (Thread/sleep 100)
        (worker/stop-executor! executor 5000)
        (let [j (first (drip/list-jobs *client* {:kind "outbox_kind" :state :completed}))]
          (is (some? j))
          (is (= {"processed" true} (:metadata j)))
          (is (= :done @side-effect)))
        (finally
          (try (worker/stop-executor! executor 1000) (catch Exception _ nil)))))

    (testing "if handler tx rolls back, job is not completed"
      (let [done (promise)
            executor (worker/start-executor!
                      {:client *client*
                       :registry {"outbox_rollback"
                                  (fn [client job]
                                    (try
                                      (drip/with-tx [tx client]
                                        (drip/complete-job! client tx (:id job))
                                        (throw (ex-info "rollback!" {})))
                                      (catch Exception e
                                        (deliver done true)
                                        (throw e))))}  ; re-throw so worker calls fail-job!
                       :poll-interval-ms 50})]
        (try
          (drip/insert-job *client* "outbox_rollback" {} {:max-attempts 1})
          (is (deref done 5000 false))
          (Thread/sleep 200)
          (worker/stop-executor! executor 5000)
          (let [j (first (drip/list-jobs *client* {:kind "outbox_rollback"}))]
            (is (some? j))
            (is (= :discarded (:state j))))
          (finally
            (try (worker/stop-executor! executor 1000) (catch Exception _ nil))))))))

(deftest record-output-stored-in-metadata
  (testing "record-output! merges output into metadata[\"output\"]"
    (let [done (promise)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"output_kind"
                                (fn [client job]
                                  (drip/with-tx [tx client]
                                    (drip/record-output! client tx (:id job) {:result 42})
                                    (drip/complete-job! client tx (:id job)))
                                  (deliver done true))}
                     :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "output_kind" {} nil)
        (is (deref done 5000 false))
        (Thread/sleep 100)
        (worker/stop-executor! executor 5000)
        (let [j (first (drip/list-jobs *client* {:kind "output_kind" :state :completed}))]
          (is (some? j))
          (is (= {"result" 42} (get (:metadata j) "output"))))
        (finally
          (try (worker/stop-executor! executor 1000) (catch Exception _ nil)))))))

(deftest ephemeral-job-deleted-on-completion
  (testing "ephemeral job is removed from DB immediately after successful completion"
    (let [done (promise)
          job-id (atom nil)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"ephemeral_kind"
                                (fn [client job]
                                  (drip/with-tx [tx client]
                                    (drip/complete-job! client tx (:id job)))
                                  (deliver done true))}
                     :poll-interval-ms 50})]
      (try
        (let [job (drip/insert-job *client* "ephemeral_kind" {:x 1} {:ephemeral true})]
          (reset! job-id (:id job))
          (is (true? (:ephemeral job))))
        (is (deref done 5000 false))
        (Thread/sleep 100)
        (worker/stop-executor! executor 5000)
        (is (nil? (drip/get-job *client* @job-id)))
        (is (empty? (drip/list-jobs *client* {:kind "ephemeral_kind" :state :completed})))
        (finally
          (try (worker/stop-executor! executor 1000) (catch Exception _ nil)))))))

(deftest ephemeral-job-failure-retries-normally
  (testing "ephemeral job that fails transitions to :retryable, not deleted"
    (let [done (promise)
          executor (worker/start-executor!
                    {:client *client*
                     :registry {"ephemeral_fail_kind"
                                (fn [_ _]
                                  (deliver done true)
                                  (throw (RuntimeException. "ephemeral failure")))}
                     :poll-interval-ms 50})]
      (try
        (drip/insert-job *client* "ephemeral_fail_kind" {} {:ephemeral true :max-attempts 3})
        (deref done 5000 nil)
        (Thread/sleep 200)
        (worker/stop-executor! executor 5000)
        (let [jobs (drip/list-jobs *client* {:kind "ephemeral_fail_kind"})]
          (is (= 1 (count jobs)))
          (is (contains? #{:retryable :available} (:state (first jobs)))))
        (finally
          (try (worker/stop-executor! executor 1000) (catch Exception _ nil)))))))
