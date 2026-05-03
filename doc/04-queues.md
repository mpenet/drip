# Queues

## Overview

Queues are named channels for job delivery. Every job belongs to exactly one queue (default: `"default"`). Queues can be paused and resumed without losing jobs. The executor polls each configured queue on every tick.

Queues are created lazily — inserting a job with `:queue "my-queue"` is enough to use that queue. Explicit `upsert-queue!` calls are only needed if you want to attach metadata or pre-register queues for management.

## Creating and updating queues

```clojure
;; Create or update (idempotent)
(drip/upsert-queue client "notifications" {:description "User notifications" :max-size 10000})
(drip/upsert-queue client "bulk" {:description "Bulk processing jobs"})

;; With explicit transaction
(drip/with-tx [tx client]
  (drip/upsert-queue! client tx "priority" {:tier "premium"}))
```

The metadata map is arbitrary — Drip stores it as JSON and doesn't interpret it. Use it for your own tooling (dashboards, monitoring scripts, etc.).

## Pausing and resuming

A paused queue's jobs remain in the database but the executor skips fetching from it. In-flight jobs already running when the queue is paused will complete normally.

```clojure
;; Pause (workers stop fetching new jobs)
(drip/pause-queue client "bulk")

;; Resume
(drip/resume-queue client "bulk")

;; Check status
(drip/list-queues client)
;; => [{:name "default" :paused-at nil :metadata {} ...}
;;     {:name "bulk"    :paused-at #inst "2026-04-30T..." :metadata {} ...}]
```

Pausing is useful for:
- Maintenance windows where you don't want new work dispatched
- Draining a queue before a deployment
- Temporarily disabling a job type without removing it from the registry

### Pausing within a transaction

```clojure
(drip/with-tx [tx client]
  (drip/pause-queue! client tx "bulk")
  (update-bulk-config! tx new-config))
```

The queue pause is atomic with your config change.

## Queue-scoped operations

All job operations support filtering by queue:

```clojure
;; List jobs in a specific queue
(drip/list-jobs client {:queue "bulk" :state :available})

;; Delete old jobs from a queue
(drip/delete-jobs client {:states [:completed]
                           :queues ["bulk"]
                           :finalized-before (.minusDays (Instant/now) 1)})
```

## Multi-queue executors

An executor can consume multiple queues. All queues share the same `:concurrency` semaphore:

```clojure
(drip/start-executor!
  {:client      client
   :registry    registry
   :queues      ["critical" "default" "bulk"]
   :concurrency 20})
```

The executor polls queues in the order listed, round-tripping through all of them on each poll tick. No priority ordering between queues — they share capacity.

For true queue-level prioritization, run separate executors with separate concurrency:

```clojure
(def critical-executor
  (drip/start-executor!
    {:client      client
     :registry    registry
     :queues      ["critical"]
     :concurrency 10
     :worker-id   "executor-critical"}))

(def default-executor
  (drip/start-executor!
    {:client      client
     :registry    registry
     :queues      ["default" "bulk"]
     :concurrency 20
     :worker-id   "executor-default"}))
```

## Queue records

`list-queues` returns a vector of maps:

| Field | Type | Description |
|---|---|---|
| `:name` | string | Queue name |
| `:created-at` | Instant | When the queue was first upserted |
| `:updated-at` | Instant | When the queue was last modified |
| `:paused-at` | Instant or nil | nil when running; set when paused |
| `:metadata` | map | User-supplied metadata (string keys) |

```clojure
(def queues (drip/list-queues client))

;; Find paused queues
(filter #(some? (:paused-at %)) queues)

;; Check if a specific queue is paused
(drip/queue-paused? client "bulk")   ; returns boolean
```

`queue-paused?` has a `!` variant for use within transactions:

```clojure
(drip/with-tx [tx client]
  (when-not (drip/queue-paused? client tx "bulk")
    (drip/insert-job! client tx "bulk_task" args)))
```

## Routing jobs to queues

Choose queues based on job characteristics:

```clojure
;; Priority-based routing
(defn enqueue-email! [client email priority]
  (drip/insert-job client "send_email" email
    :queue (case priority
             :high    "critical"
             :normal  "default"
             :low     "bulk")))

;; Tenant-based routing (separate resource pools)
(defn enqueue-for-tenant! [client tenant-id job-kind args]
  (drip/insert-job client job-kind args
    :queue (str "tenant-" tenant-id)))

;; Resource-type routing
(defn enqueue-job! [client kind args]
  (let [queue (cond
                (contains? #{:resize_image :transcode_video} kind) "media"
                (contains? #{:send_email :send_sms} kind)          "notifications"
                :else                                               "default")]
    (drip/insert-job client (name kind) args :queue queue)))
```
