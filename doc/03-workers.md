# Workers and the Executor

## Overview

The executor is a background process that polls queues, claims jobs atomically, dispatches them to worker functions, and manages their lifecycle (completion, retries, cleanup). Start one with `start-executor!` and stop it with `stop-executor!`.

## Registry

The registry maps job kind strings to handler functions:

```clojure
(def registry
  {"send_email"     email-handler
   "send_sms"       sms-handler
   "process_upload" upload-handler})
```

A handler is a plain function of two arguments: `[client job]`.

```clojure
(defn email-handler [client job]
  (let [{:keys [to subject body]} (:args job)]
    (send-email! to subject body)))
```

- Handlers must explicitly call `complete-job!`, `snooze-job!`, etc. to manage job state.
- Throw any `Throwable` → worker records the error and retries (or discards).

## Starting the executor

```clojure
(def executor
  (drip/start-executor!
    {:client   client
     :registry registry}))
```

### All options

```clojure
(drip/start-executor!
  {:client           client       ; required
   :registry         registry     ; required

   ;; Queues
   :queues           ["default"]  ; queues to consume (default ["default"])

   ;; Concurrency
   :concurrency      10           ; max simultaneous in-flight jobs (default 10)

   ;; Polling
   :poll-interval-ms 1000         ; ms between polls (default 1000)
   :worker-id        "worker-1"   ; unique ID for this executor (default random UUID)

   ;; Retries
   :retry-policies   {:default  my-policy        ; :default = fallback policy for all kinds
                      "slow_job" lenient-policy}  ; per-kind overrides

   ;; Timeouts
   :job-timeouts     {:default  nil       ; global timeout; nil = no limit
                      "slow_job" "2m"}  ; per-kind override; duration strings or ms

   ;; Maintenance
   :rescue-after     {:default "1h"  ; :default = global threshold (duration string or ms)
                      "slow" "4h"}  ; per-queue overrides; nil = disable rescue
   :retention        {:default  {:completed "24h"  ; 1 day
                                 :cancelled "24h"  ; 1 day
                                 :discarded "7d"}  ; 7 days
                      "fast"    {:completed "1h"}}}) ; per-queue override
```

## Stopping the executor

### Graceful shutdown

Waits for in-flight jobs to finish, up to a timeout:

```clojure
(drip/stop-executor! executor)          ; default 30s timeout
(drip/stop-executor! executor 60000)    ; custom timeout in ms
;; returns true if clean, false if timed out
```

In-flight jobs that finish before the timeout are marked `:completed` or `:retryable` normally.

### Immediate shutdown

Interrupts all in-flight virtual threads immediately:

```clojure
(drip/stop-and-cancel! executor)
```

In-flight jobs remain in `:running` state. On the next executor startup (or if another executor is running), `rescue-stuck-jobs!` will requeue them based on `:rescue-after`.

Use this for fast deploys where you can afford jobs to re-run.

## How jobs are processed

1. Every `:poll-interval-ms` milliseconds, the executor calls `promote-scheduled-jobs!` (moves scheduled/retryable jobs whose time has come to `:available`).
2. For each queue, the executor claims up to `concurrency` available jobs atomically (using `FOR UPDATE SKIP LOCKED`), setting their state to `:running`.
3. Each job is dispatched to a virtual thread. The semaphore ensures no more than `concurrency` jobs run simultaneously across all queues.
4. On handler success: job → `:completed`.
5. On handler exception: error is recorded; if `attempt < max_attempts`, job → `:retryable` with `scheduled_at` set by retry policy; otherwise → `:discarded`.
6. On timeout: job thread is interrupted and job → `:retryable`/`:discarded`.

## Concurrency

`:concurrency` controls the maximum number of simultaneously in-flight jobs across all queues. It is implemented as a semaphore that also gates how many jobs are fetched from the database, preventing over-claiming.

Jobs run on virtual threads (Java 21+). High concurrency values (hundreds) are practical for IO-bound work. For CPU-bound work, keep concurrency near your available processor count.

```clojure
;; IO-heavy (email sending, HTTP calls, etc.)
{:concurrency 100}

;; CPU-heavy (image processing, compression, etc.)
{:concurrency (.availableProcessors (Runtime/getRuntime))}

;; Multi-queue with natural separation
{:queues      ["critical" "default" "bulk"]
 :concurrency 50}
```

## Retry policies

A retry policy is a plain function `(fn [attempt] java.time.Instant)`. The `attempt` argument is 1-based (it reflects the attempt that just failed).

### Default policy

Exponential backoff: `attempt^4` seconds ± 10% jitter.

| Attempt | Approx delay |
|---|---|
| 1 | 1s |
| 2 | 16s |
| 3 | 81s |
| 4 | 4.3m |
| 5 | 10.4m |
| 10 | 27.8h |
| 20 | ~160 days |

### Built-in policy constructors

Drip ships several policy constructors. All duration arguments accept strings (`"30s"`, `"5m"`, `"1h"`) or raw milliseconds.

```clojure
;; Fixed delay
(drip/constant-retry-policy "30s")                        ; always 30s
(drip/constant-retry-policy "30s" :jitter 0.1)            ; 30s ± 10%

;; Linear: base * attempt
(drip/linear-retry-policy "10s")                          ; 10s, 20s, 30s, …
(drip/linear-retry-policy "10s" :max "5m")                ; capped at 5m
(drip/linear-retry-policy "10s" :max "5m" :jitter 0.1)   ; + 10% jitter

;; Exponential: base * multiplier^(attempt-1)
(drip/exponential-retry-policy "1s")                                           ; 1s, 2s, 4s, … capped at 1h
(drip/exponential-retry-policy "1s" :multiplier 3.0)                           ; 1s, 3s, 9s, …
(drip/exponential-retry-policy "1s" :multiplier 2.0 :max "30m")                ; capped at 30m
(drip/exponential-retry-policy "1s" :multiplier 2.0 :max "30m" :jitter 0.15)  ; + 15% jitter

;; Immediate: no delay (useful for tests or idempotent fast-fail jobs)
(drip/immediate-retry-policy)
```

### Custom policies

Any function `(fn [attempt] java.time.Instant)` works as a policy:

```clojure
(defn my-policy [attempt]
  (.plusSeconds (Instant/now) (* 30 attempt)))
```

### Per-kind policies

Use `:retry-policies` with a `:default` key for the global policy and kind-string keys for per-kind overrides:

```clojure
(drip/start-executor!
  {:client         client
   :registry       registry
   :retry-policies {:default   (drip/exponential-retry-policy "5s" :multiplier 2.0 :max "1h")
                    "fast_fail" (drip/constant-retry-policy "2s")
                    "slow_job"  (drip/linear-retry-policy "1m" :max "1h")}})
```

## Job timeouts

Limit how long a single job can run. When exceeded, the job thread is interrupted and the job is retried or discarded:

```clojure
(drip/start-executor!
  {:client     client
   :registry   registry
   :job-timeouts {:default       "30s"  ; 30s for all jobs (nil = no timeout)
                  "slow_report"  "2m"   ; 2 minutes for this kind
                  "quick_notify" "5s"}}) ; 5 seconds for this kind
```

`:default` is the global timeout applied to any kind not listed explicitly. If `:default` is nil (or absent), there is no global timeout. Per-kind entries override `:default` for that kind. Values accept duration strings (`"30s"`, `"2m"`) or plain millisecond numbers.

## Retention and cleanup

The executor automatically deletes old finalized jobs on each poll cycle. `:retention` is a unified map: the `:default` key holds the global `{state → ms}` windows, and queue-name string keys hold per-queue overrides merged on top of `:default`.

```clojure
{:retention {:default  {:completed "24h"  ; delete completed jobs after 1 day
                        :cancelled "24h"  ; delete cancelled jobs after 1 day
                        :discarded "7d"}}} ; delete discarded jobs after 7 days
```

Values accept duration strings (`"1h"`, `"30m"`, `"7d"`) or plain millisecond numbers.

Set a state to `nil` to disable cleanup for it. Set `:retention nil` to disable all automatic cleanup.

Default: `{:default {:completed "24h" :cancelled "24h" :discarded "7d"}}`.

### Per-queue retention

Add queue-name string keys to `:retention`. Each entry is merged on top of `:default` for that queue — only override the states that differ:

```clojure
{:retention {:default  {:completed "24h"   ; global: 1 day
                        :discarded "7d"}   ; global: 7 days
             "fast"    {:completed "1h"}   ; fast queue: 1h completed
             "archive" {:discarded nil}    ; archive queue: never delete discarded
             "critical" {:completed "30d"  ; critical queue: 30 days completed
                         :discarded "30d"}}}  ; critical queue: 30 days discarded
```

Queues not listed use `:default`. Setting a state to `nil` in a queue entry disables cleanup for that state on that queue only.

## PostgreSQL LISTEN/NOTIFY

On PostgreSQL, the executor automatically opens a second connection that LISTENs on the `drip_insert` channel. When any process inserts a job, `pg_notify` fires and the executor polls immediately instead of waiting for the next `:poll-interval-ms`. This reduces job latency to near-zero.

No configuration needed — it happens automatically for PostgreSQL clients.

## Multiple executors

You can run multiple executors in the same process, or across multiple processes, against the same database. They coordinate through the database: `FOR UPDATE SKIP LOCKED` ensures each job is claimed by exactly one worker.

```clojure
;; Separate executors for different queues with different concurrency
(def critical-executor
  (drip/start-executor!
    {:client      client
     :registry    registry
     :queues      ["critical"]
     :concurrency 5
     :worker-id   "critical-worker"}))

(def bulk-executor
  (drip/start-executor!
    {:client      client
     :registry    registry
     :queues      ["bulk"]
     :concurrency 50
     :worker-id   "bulk-worker"}))
```

Each executor maintains its own semaphore and scheduler. `rescue-stuck-jobs!` uses the worker's own `worker-id` to avoid interfering with jobs owned by other workers — actually, it rescues *all* stuck jobs regardless of which worker claimed them, so any live executor can rescue orphaned jobs from a crashed process.

## Unknown job kinds

If a job is fetched whose `:kind` is not in the registry, the executor calls `discard-job!` — the job is moved to `:discarded` without consuming a retry. This handles kind renames, dead code removal, and deployment skew.

## Explicit state management

Handlers own their own job state. Call `complete-job!`, `snooze-job!`, etc. directly:

```clojure
{"process_order" (fn [client job]
                   (drip/with-tx [tx client]
                     (update-inventory! tx (:args job))
                     (drip/complete-job! client tx (:id job))))}
```

If the handler throws, the error is recorded and the job retried normally regardless of whether `complete-job!` was called.
