# Workers

## Overview

The worker is a background process that polls queues, claims jobs atomically, dispatches them to handler functions, and manages their lifecycle (completion, retries, cleanup). Start one with `start-worker!` and stop it with `stop-worker!`.

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

## Starting the worker

```clojure
(def worker
  (drip/start-worker!
    {:client   client
     :registry registry}))
```

### All options

```clojure
(drip/start-worker!
  {:client           client       ; required
   :registry         registry     ; required

   ;; Queues
   :queues           ["default"]  ; queues to consume (default ["default"])

   ;; Concurrency
   :concurrency      10           ; max simultaneous in-flight jobs (default 10)

   ;; Polling
   :poll-interval "1s"            ; interval between polls (default "1s")
   :worker-id        "worker-1"   ; unique ID for this worker (default random UUID)

   ;; Executor
   :executor         my-executor  ; optional ExecutorService; drip shuts it down on stop

   ;; Retries
   :retry-policies   {:default  my-policy        ; :default = fallback policy for all kinds
                      "slow_job" lenient-policy}  ; per-kind overrides

   ;; Timeouts
   :job-timeouts     {:default  nil       ; global timeout; nil = no limit
                      "slow_job" "2m"}   ; per-kind override; duration strings or ms

   ;; Observability
   :event-fn         (fn [event] ...)})  ; called for every worker event (metrics, tracing, logging)
```

Rescue, retention, and reindex are handled by the [maintenance worker](#maintenance-worker).

## Stopping the worker

### Graceful shutdown

Waits for in-flight jobs to finish, up to a timeout:

```clojure
(drip/stop-worker! executor)                     ; default 30s timeout
(drip/stop-worker! executor :timeout "60s")      ; custom timeout
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

1. Every `:poll-interval`, the worker calls `promote-scheduled-jobs!` (moves scheduled/retryable jobs whose time has come to `:available`).
2. For each queue, the worker claims up to `concurrency` available jobs atomically (using `FOR UPDATE SKIP LOCKED`), setting their state to `:running`.
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

## Custom executor

By default, drip creates a virtual-thread-per-task executor for job dispatch. Pass `:executor` to supply your own `ExecutorService` — useful for custom thread factories, MDC propagation, metrics instrumentation, or fixed-thread pools for CPU-bound work.

```clojure
(import '(java.util.concurrent Executors))

(def my-exec (Executors/newVirtualThreadPerTaskExecutor))

(def w (drip/start-worker!
         {:client   client
          :registry registry
          :executor my-exec}))

;; drip owns shutdown — stop-worker! shuts down my-exec
(drip/stop-worker! w)
```

drip always shuts down the supplied executor on `stop-worker!` / `stop-and-cancel!`.

## Retry policies

A retry policy is a plain function `(fn [attempt] java.time.Instant)`. The `attempt` argument is 1-based (it reflects the attempt that just failed).

### Default policy

Exponential backoff: base 1s, multiplier 2, max 1h, ±10% jitter. Equivalent to `(drip/exponential-retry-policy "1s")`.

| Attempt | Approx delay |
|---|---|
| 1 | ~1s |
| 2 | ~2s |
| 3 | ~4s |
| 4 | ~8s |
| 5 | ~16s |
| 6 | ~32s |
| 7 | ~64s |
| 10 | ~8.5m |
| 14+ | ~1h (capped) |

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
(drip/start-worker!
  {:client         client
   :registry       registry
   :retry-policies {:default   (drip/exponential-retry-policy "5s" :multiplier 2.0 :max "1h")
                    "fast_fail" (drip/constant-retry-policy "2s")
                    "slow_job"  (drip/linear-retry-policy "1m" :max "1h")}})
```

## Job timeouts

Limit how long a single job can run. When exceeded, the job thread is interrupted and the job is retried or discarded:

```clojure
(drip/start-worker!
  {:client     client
   :registry   registry
   :job-timeouts {:default       "30s"  ; 30s for all jobs (nil = no timeout)
                  "slow_report"  "2m"   ; 2 minutes for this kind
                  "quick_notify" "5s"}}) ; 5 seconds for this kind
```

`:default` is the global timeout applied to any kind not listed explicitly. If `:default` is nil (or absent), there is no global timeout. Per-kind entries override `:default` for that kind. Values accept duration strings (`"30s"`, `"2m"`) or plain millisecond numbers.

## Observability

Pass `:event-fn` to receive a callback for every worker event. Useful for metrics, tracing, and structured logging. Exceptions thrown by the fn are swallowed — it will never affect job processing.

```clojure
(drip/start-worker!
  {:client   client
   :registry registry
   :event-fn (fn [{:keys [type kind queue job-id attempt duration-ms error]}]
               (case type
                 :s-exp.drip.job/complete (record-duration! kind duration-ms)
                 :s-exp.drip.job/fail     (increment-counter! :job-failures {:kind kind})
                 :s-exp.drip.job/timeout  (increment-counter! :job-timeouts {:kind kind})
                 nil))})
```

### Worker event types

| Type | Extra keys |
|---|---|
| `:s-exp.drip.job/start` | — |
| `:s-exp.drip.job/complete` | `:duration-ms` |
| `:s-exp.drip.job/fail` | `:duration-ms` `:error` |
| `:s-exp.drip.job/timeout` | `:duration-ms` `:error` |
| `:s-exp.drip.job/discard` | — |
| `:s-exp.drip.poll/fetched` | `:count` |

All job events carry `:worker-id`, `:queue`, `:kind`, `:job-id`, and `:attempt`.

### Maintenance worker event types

The maintenance worker also accepts `:event-fn`. Events fire after each task completes (or errors):

| Type | Extra keys |
|---|---|
| `:s-exp.drip.maintenance/rescue` | `:duration-ms` (`:error` on failure) |
| `:s-exp.drip.maintenance/retention` | `:duration-ms` (`:error` on failure) |
| `:s-exp.drip.maintenance/reindex` | `:duration-ms` `:results` (`:error` on failure) |

## Maintenance worker

Rescue, retention cleanup, and index maintenance run in a separate `MaintenanceWorker`, independent of the job executor. Each task runs on its own thread so a slow operation (e.g. a large DELETE sweep or a long REINDEX) does not block the others.

```clojure
(def maintenance
  (drip/start-maintenance-worker!
    {:client client

     ;; Rescue stuck jobs (running longer than threshold)
     :rescue-after     {:default "1h"   ; global threshold; duration string or ms
                        "slow"   "4h"}  ; per-queue overrides; nil = disable for that queue
     :rescue-interval  "1m"             ; how often to run rescue (default "1m")
     :retry-policy     drip/default-retry-policy  ; policy used when rescuing

     ;; Retention cleanup
     :retention        {:default  {:completed "24h"
                                   :cancelled "24h"
                                   :discarded "7d"}
                        "fast"    {:completed "1h"}      ; per-queue override
                        "archive" {:discarded nil}}       ; disable discard cleanup
     :retention-interval "1m"           ; how often to run cleanup (default "1m")

     ;; Reindex (PostgreSQL only, no-op on others)
     :reindex-interval "24h"            ; nil = disabled (default)

     ;; Which queues rescue/retention apply to
     :queues ["default" "fast" "slow"]}))

(drip/stop-maintenance-worker! maintenance)         ; default 5s timeout
(drip/stop-maintenance-worker! maintenance 10000)   ; custom timeout
```

All interval values (`:rescue-interval`, `:retention-interval`, `:reindex-interval`) accept duration strings (`"1m"`, `"24h"`) or raw millisecond numbers.

Set `:rescue-after nil` to disable rescue entirely. Set `:retention nil` to disable all retention cleanup.

### Rescue

Rescue finds jobs that have been in `:running` state longer than the configured threshold — indicating the worker that claimed them crashed or was killed — and moves them to `:retryable` or `:discarded` (if max attempts exhausted). The rescue threshold should be longer than your longest expected job duration.

### Retention

Retention deletes finalized jobs older than the configured windows. `:default` is the global `{state → duration}` map. Queue-name string keys are per-queue overrides merged on top of `:default`:

```clojure
{:retention {:default   {:completed "24h" :discarded "7d"}
             "fast"     {:completed "1h"}      ; override only completed for this queue
             "archive"  {:discarded nil}}}      ; never delete discarded on archive queue
```

Values accept duration strings or raw milliseconds. Set a state to `nil` to disable cleanup for that state.

### Reindex (PostgreSQL only)

Periodically runs `REINDEX INDEX CONCURRENTLY` on drip's job indexes to recover index bloat. No-op on MariaDB and SQLite.

**Important:** with multiple nodes, you are responsible for leader election — running reindex on every node simultaneously is safe but wasteful. Disable it on non-leader nodes by not passing `:reindex-interval`.

Skips any index where leftover `_ccnew`/`_ccold` artifacts from a previous failed concurrent reindex are detected — safe to retry on the next run.

Returns `{index-name-keyword => :reindexed | :skipped | :not-found}` per run (logged automatically).

## PostgreSQL LISTEN/NOTIFY

On PostgreSQL, the worker automatically opens a second connection that LISTENs on the `drip_insert` channel. When any process inserts a job, `pg_notify` fires and the worker polls immediately instead of waiting for the next `:poll-interval`. This reduces job latency to near-zero.

No configuration needed — it happens automatically for PostgreSQL clients.

## Multiple workers

You can run multiple workers in the same process, or across multiple processes, against the same database. They coordinate through the database: `FOR UPDATE SKIP LOCKED` ensures each job is claimed by exactly one worker.

```clojure
;; Separate workers for different queues with different concurrency
(def critical-worker
  (drip/start-worker!
    {:client      client
     :registry    registry
     :queues      ["critical"]
     :concurrency 5
     :worker-id   "critical-worker"}))

(def bulk-worker
  (drip/start-worker!
    {:client      client
     :registry    registry
     :queues      ["bulk"]
     :concurrency 50
     :worker-id   "bulk-worker"}))
```

Each worker maintains its own semaphore and scheduler. Any live maintenance worker can rescue orphaned jobs from a crashed process — rescue operates across all workers, not just the one that originally claimed the job.

## Unknown job kinds

If a job is fetched whose `:kind` is not in the registry, the worker calls `discard-job!` — the job is moved to `:discarded` without consuming a retry. This handles kind renames, dead code removal, and deployment skew.

## Explicit state management

Handlers own their own job state. Call `complete-job!`, `snooze-job!`, etc. directly:

```clojure
{"process_order" (fn [client job]
                   (drip/with-tx [tx client]
                     (update-inventory! tx (:args job))
                     (drip/complete-job! client tx (:id job))))}
```

If the handler throws, the error is recorded and the job retried normally regardless of whether `complete-job!` was called.
