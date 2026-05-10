# Periodic Jobs

## Overview

Periodic jobs insert a new job on a fixed-interval schedule. Each interval fires at most one job — if a job from the previous period is still in an active state, the insert is a no-op (unique constraint conflict). This gives you durable, at-most-once-per-period scheduled work without a separate cron daemon.

## Quick example

```clojure
(def scheduler
  (drip/start-periodic-jobs! client
    [{:kind   "daily_report"
      :args   {:type "summary"}
      :period "24h"
      :queue  "default"}
     {:kind   "hourly_cleanup"
      :args   {}
      :period "1h"
      :queue  "maintenance"}]))

;; On shutdown
(drip/stop-periodic-jobs! scheduler)
```

## Spec format

Each spec is a map:

| Key | Type | Required | Description |
|---|---|---|---|
| `:kind` | string | yes | Job kind (must be in worker registry) |
| `:args` | map | yes | Job arguments (can be `{}`) |
| `:period` | duration | yes | Interval between jobs |
| `:queue` | string | no | Target queue (default `"default"`) |
| `:opts` | map | no | Extra insert opts (priority, tags, metadata, max-attempts) |

## Duration strings

The `:period` field accepts a duration number (milliseconds) or a string:

| String | Meaning |
|---|---|
| `"1h"` | 1 hour |
| `"30m"` | 30 minutes |
| `"90s"` | 90 seconds |
| `"1d"` | 1 day |
| `"500ms"` | 500 milliseconds |

You can also pass a plain long (milliseconds): `3600000` = 1 hour.

## How deduplication works

Each periodic spec computes a unique key from:
- Job kind
- Queue name (if `:by-queue?` is true — always true for periodic jobs)
- Period window (floor of current epoch to period boundary)

If a job with that key exists in an active state (`:available`, `:pending`, `:running`, `:scheduled`, `:retryable`, or `:completed`), the new insert throws a constraint conflict. The periodic executor catches SQLState `23505` (PG) / `23000` (MariaDB/SQLite) silently and moves on. Any other error is logged.

This means:
- Multiple executors can run periodic specs without duplicating jobs
- If a periodic job fails and is retrying, the next period's insert is blocked until the retrying job finishes or is discarded
- Missing a period (e.g. all executors down) does not cause catch-up bursts — the next period fires one job as normal

## Multiple periods in one executor

```clojure
(drip/start-periodic-jobs! client
  [{:kind "heartbeat"          :args {} :period "1m"}
   {:kind "hourly_metrics"     :args {} :period "1h"}
   {:kind "daily_digest"       :args {} :period "24h"}
   {:kind "weekly_report"      :args {} :period "7d"}])
```

Each spec gets its own scheduled task in the underlying `ScheduledExecutorService`. They fire independently.

## With extra options

```clojure
(drip/start-periodic-jobs! client
  [{:kind  "batch_process"
    :args  {:source "s3://bucket/path"}
    :period "6h"
    :queue  "batch"
    :opts   {:priority     2
             :max-attempts 3
             :tags         ["periodic" "batch"]
             :metadata     {:owner "data-team"}}}])
```

## Lifecycle

`start-periodic-jobs!` returns a `java.util.concurrent.ScheduledExecutorService`. Each spec fires at a fixed rate starting immediately (at time 0, then every period thereafter).

`stop-periodic-jobs!` calls `shutdown` on the scheduler — no more inserts will fire after this returns, but any currently-running insert completes first.

```clojure
;; Typical startup/shutdown
(defstate scheduler
  :start (drip/start-periodic-jobs! client specs)
  :stop  (drip/stop-periodic-jobs! scheduler))
```

## Periodic jobs vs scheduled jobs

| | Periodic jobs | Scheduled jobs |
|---|---|---|
| Trigger | Time interval (recurring) | One-time future instant |
| Insert | From `start-periodic-jobs!` | From `insert-job` with `:scheduled-at` |
| Dedup | Automatic (unique per period) | Manual (`:unique-opts`) |
| Use case | Recurring maintenance, reports | Delayed notifications, reminders |

To implement cron-like behaviour (specific times, not just intervals), insert a scheduled job from within a periodic handler:

```clojure
;; Periodic job that fires every hour, then schedules work for the next midnight
{"hourly_scheduler"
 (fn [client _job]
   (let [midnight (next-midnight-instant)]
     (drip/insert-job client "nightly_report" {} :scheduled-at midnight
       :unique-opts {:by-period "24h"})))}
```

## Combining periodic and regular executors

Periodic and regular executors are independent. A common setup:

```clojure
;; Worker processes jobs
(def worker
  (drip/start-worker!
    {:client   client
     :registry registry
     :queues   ["default" "maintenance"]}))

;; Periodic executor inserts recurring jobs
(def scheduler
  (drip/start-periodic-jobs! client
    [{:kind "hourly_cleanup" :args {} :period "1h" :queue "maintenance"}
     {:kind "daily_report"   :args {} :period "24h"}]))
```

The periodic executor only inserts jobs; the regular executor processes them.
