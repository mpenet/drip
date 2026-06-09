(ns s-exp.drip.maintenance
  "Optional maintenance worker and tasks.

   Use start-maintenance-worker! to run periodic maintenance tasks on a
   schedule. Caller is responsible for leader election when running multiple
   nodes — running on every node simultaneously is safe but wasteful for
   reindexing, and produces redundant work for cleanup/rescue."
  (:require [clojure.tools.logging :as log]
            [s-exp.drip.client :as client]
            [s-exp.drip.db :as db]
            [s-exp.drip.job :as job]
            [s-exp.duration :as duration])
  (:import (java.time Instant)
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit)))

(set! *warn-on-reflection* true)

(defn- emit! [event-fn event]
  (when event-fn
    (try (event-fn event) (catch Exception _))))

;; ---------------------------------------------------------------------------
;; Retention / cleanup
;; ---------------------------------------------------------------------------

(def default-retention
  {:completed "24h"
   :cancelled "24h"
   :discarded "7d"})

(defn- run-cleaner [c tx retention queues]
  (let [now (Instant/now)
        global (get retention :default)
        per-queue (dissoc retention :default)
        delete! (fn [states-ms qs]
                  (doseq [[state dur] states-ms]
                    (when dur
                      (client/delete-jobs! c tx
                                           (cond-> {:states [state]
                                                    :finalized-before (.minusMillis now (long (duration/duration dur)))}
                                             (seq qs) (assoc :queues (vec qs)))))))]
    (doseq [[q-name q-retention] per-queue]
      (delete! (merge global q-retention) [q-name]))
    (when (seq global)
      (let [overridden (set (keys per-queue))
            global-qs (seq (remove overridden queues))]
        (when (or (empty? overridden) (seq global-qs))
          (delete! global global-qs))))))

;; ---------------------------------------------------------------------------
;; Rescue stuck jobs
;; ---------------------------------------------------------------------------

(defn- run-rescue [client tx rescue-after retry-policy queues]
  (let [now (Instant/now)
        default-dur (get rescue-after :default)
        per-queue (dissoc rescue-after :default)
        rescue! (fn [dur qs]
                  (when dur
                    (client/rescue-stuck-jobs! client tx
                                               (.minusMillis now (long (duration/duration dur)))
                                               retry-policy
                                               qs)))]
    (doseq [[q-name q-dur] per-queue]
      (rescue! q-dur [q-name]))
    (when default-dur
      (let [overridden (set (keys per-queue))
            global-qs (seq (remove overridden queues))]
        (when (or (empty? overridden) (seq global-qs))
          (rescue! default-dur global-qs))))))

;; ---------------------------------------------------------------------------
;; TTL expiry
;; ---------------------------------------------------------------------------

(defn- run-ttl-expiry [c tx]
  (client/expire-ttl-jobs! c tx))

;; ---------------------------------------------------------------------------
;; Reindex
;; ---------------------------------------------------------------------------

(defn reindex!
  "Rebuilds drip job indexes to recover bloat. PostgreSQL only — runs
   REINDEX INDEX CONCURRENTLY for each index. No-op on MariaDB/SQLite.

   Skips indexes where leftover _ccnew/_ccold artifacts exist from a
   previous failed concurrent reindex — safe to retry later.

   Returns a map of {index-name-keyword => :reindexed | :skipped | :not-found}.

   Must NOT be called inside a transaction (REINDEX CONCURRENTLY requirement)."
  [client]
  (try
    (let [results (client/reindex! client)]
      (doseq [[idx status] results]
        (case status
          :reindexed (log/info "drip: reindexed" (name idx))
          :skipped (log/warn "drip: skipped reindex (artifact present)" (name idx))
          :not-found (log/debug "drip: index not found, skipping" (name idx))))
      results)
    (catch Exception e
      (log/error e "drip: reindex failed")
      (throw e))))

;; ---------------------------------------------------------------------------
;; Maintenance worker lifecycle
;; ---------------------------------------------------------------------------

(defrecord MaintenanceWorker
           [client
            queues
            rescue-after
            retry-policy
            retention
            reindex-interval
            ttl-interval
            event-fn
            ^ScheduledExecutorService rescue-scheduler
            ^ScheduledExecutorService retention-scheduler
            ^ScheduledExecutorService reindex-scheduler
            ^ScheduledExecutorService ttl-scheduler
            running?])

(defn- schedule-when! [^ScheduledExecutorService sched interval f]
  (when (and sched interval)
    (let [ms (long (duration/duration interval))]
      (.scheduleWithFixedDelay sched ^Runnable f 0 ms TimeUnit/MILLISECONDS))))

(defn start-maintenance-worker!
  "Starts a maintenance worker that periodically runs rescue, retention, and reindex tasks,
   each on its own scheduler thread so slow tasks don't block each other.

   Required options:
     :client  - client from make-client (mariadb/postgres/sqlite)

   Optional options:
     :queues                - queues to apply rescue/retention to (default [\"default\"])
     :rescue-after          - unified rescue config map. :default is the global stuck threshold;
                              queue-name string keys override per queue. Each value is a duration:
                              ms number or duration string (e.g. \"1h\", \"30m\").
                              Set :default to nil to disable global rescue. Set to nil to disable rescue.
                              Default: {:default \"1h\"}
                              Example: {:default \"1h\" \"slow\" \"4h\" \"fast\" \"15m\"}
     :rescue-interval       - how often to run rescue; duration string or ms (default \"1m\")
     :retry-policy          - policy fn used when rescuing stuck jobs (default: exponential backoff)
     :retention             - unified retention config map. Keys are :default (global {state->duration}
                              map) or queue-name strings (per-queue overrides merged with :default).
                              Set a state to nil to disable cleanup for it. Set to nil to disable all.
                              Default: {:default {:completed \"24h\" :cancelled \"24h\" :discarded \"7d\"}}
                              Example: {:default {:completed \"24h\" :discarded \"7d\"}
                                        \"fast\"   {:completed \"1h\"}
                                        \"archive\" {:discarded nil}}
     :retention-interval    - how often to run retention cleanup; duration string or ms (default \"1m\")
     :reindex-interval      - how often to run reindex; duration string or ms. nil = disabled (default).
                              PostgreSQL only; no-op on other databases.
                              Caller is responsible for leader election when using multiple nodes.
                              Example: \"24h\"
     :ttl-interval          - how often to expire TTL-exceeded jobs; duration string or ms.
                              nil = disabled (default).
                              Targets :available, :scheduled, and :retryable jobs whose created_at + ttl_ms <= now.
                              Example: \"1m\"
     :event-fn              - optional (fn [event]) called after each maintenance task.
                              Exceptions are swallowed. Event types:
                                :s-exp.drip.maintenance/rescue    - after rescue run
                                :s-exp.drip.maintenance/retention - after retention run
                                :s-exp.drip.maintenance/reindex   - after reindex run
                              All events carry :duration-ms. Error events carry :error (Exception).
                              Reindex success events carry :results ({index-kw => status}).
                              TTL expiry success events carry :count (number of expired jobs).

   Returns a MaintenanceWorker record. Stop with stop-maintenance-worker!."
  [{:keys [client queues
           rescue-after rescue-interval
           retry-policy
           retention retention-interval
           reindex-interval
           ttl-interval
           event-fn]
    :or {queues ["default"]
         rescue-after {:default "1h"}
         rescue-interval "1m"
         retry-policy job/default-retry-policy
         retention {:default default-retention}
         retention-interval "1m"}}]
  (let [rescue-scheduler (when rescue-after (Executors/newSingleThreadScheduledExecutor))
        retention-scheduler (when retention (Executors/newSingleThreadScheduledExecutor))
        reindex-scheduler (when reindex-interval (Executors/newSingleThreadScheduledExecutor))
        ttl-scheduler (when ttl-interval (Executors/newSingleThreadScheduledExecutor))
        running? (atom true)]
    (schedule-when!
     rescue-scheduler rescue-interval
     (fn []
       (when @running?
         (let [t0 (System/currentTimeMillis)]
           (try
             (db/with-tx [tx client]
               (run-rescue client tx rescue-after retry-policy queues))
             (emit! event-fn {:type :s-exp.drip.maintenance/rescue
                              :duration-ms (- (System/currentTimeMillis) t0)})
             (catch Exception e
               (log/error e "drip: rescue error")
               (emit! event-fn {:type :s-exp.drip.maintenance/rescue
                                :duration-ms (- (System/currentTimeMillis) t0)
                                :error e})))))))
    (schedule-when!
     retention-scheduler retention-interval
     (fn []
       (when @running?
         (let [t0 (System/currentTimeMillis)]
           (try
             (db/with-tx [tx client]
               (run-cleaner client tx retention queues))
             (emit! event-fn {:type :s-exp.drip.maintenance/retention
                              :duration-ms (- (System/currentTimeMillis) t0)})
             (catch Exception e
               (log/error e "drip: retention error")
               (emit! event-fn {:type :s-exp.drip.maintenance/retention
                                :duration-ms (- (System/currentTimeMillis) t0)
                                :error e})))))))
    (schedule-when!
     reindex-scheduler reindex-interval
     (fn []
       (when @running?
         (let [t0 (System/currentTimeMillis)]
           (try
             (let [results (reindex! client)]
               (emit! event-fn {:type :s-exp.drip.maintenance/reindex
                                :duration-ms (- (System/currentTimeMillis) t0)
                                :results results}))
             (catch Exception e
               (log/error e "drip: reindex error")
               (emit! event-fn {:type :s-exp.drip.maintenance/reindex
                                :duration-ms (- (System/currentTimeMillis) t0)
                                :error e})))))))
    (schedule-when!
     ttl-scheduler ttl-interval
     (fn []
       (when @running?
         (let [t0 (System/currentTimeMillis)]
           (try
             (let [n (db/with-tx [tx client]
                       (run-ttl-expiry client tx))]
               (emit! event-fn {:type :s-exp.drip.maintenance/ttl-expiry
                                :duration-ms (- (System/currentTimeMillis) t0)
                                :count n}))
             (catch Exception e
               (log/error e "drip: ttl-expiry error")
               (emit! event-fn {:type :s-exp.drip.maintenance/ttl-expiry
                                :duration-ms (- (System/currentTimeMillis) t0)
                                :error e})))))))
    (map->MaintenanceWorker
     {:client client
      :queues queues
      :rescue-after rescue-after
      :retry-policy retry-policy
      :retention retention
      :reindex-interval reindex-interval
      :ttl-interval ttl-interval
      :event-fn event-fn
      :rescue-scheduler rescue-scheduler
      :retention-scheduler retention-scheduler
      :reindex-scheduler reindex-scheduler
      :ttl-scheduler ttl-scheduler
      :running? running?})))

(defn stop-maintenance-worker!
  "Shuts down the maintenance worker.

   Options (keyword args):
     :timeout - max time to wait for in-progress tasks; duration string or ms (default \"5s\")

   Returns true if all schedulers shut down cleanly, false if any timed out."
  [{:keys [^ScheduledExecutorService rescue-scheduler
           ^ScheduledExecutorService retention-scheduler
           ^ScheduledExecutorService reindex-scheduler
           ^ScheduledExecutorService ttl-scheduler
           running?]}
   & {:keys [timeout]
      :or {timeout "5s"}}]
  (reset! running? false)
  (doseq [^ScheduledExecutorService s [rescue-scheduler retention-scheduler reindex-scheduler ttl-scheduler]
          :when s]
    (.shutdown s))
  (let [timeout-ms (long (duration/duration timeout))]
    (reduce (fn [clean? ^ScheduledExecutorService s]
              (if s
                (and clean? (.awaitTermination s timeout-ms TimeUnit/MILLISECONDS))
                clean?))
            true
            [rescue-scheduler retention-scheduler reindex-scheduler ttl-scheduler])))
