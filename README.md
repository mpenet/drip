# Drip [![Tests](https://github.com/mpenet/drip/actions/workflows/test.yml/badge.svg)](https://github.com/mpenet/drip/actions/workflows/test.yml) [![Clojars Project](https://img.shields.io/clojars/v/com.s-exp/drip.svg)](https://clojars.org/com.s-exp/drip) 


<p align="center"><img align="center" width="150" height="150"  src="https://github.com/user-attachments/assets/85264171-b386-4a29-85f3-1a8070f0389d" /></p>

**Reliable background jobs for Clojure. Backed by your existing database.**

Drip is a transactional job queue for MariaDB, PostgreSQL, and SQLite. Jobs enqueue inside your application's own transactions — if your transaction commits, the job is guaranteed to run; if it rolls back, the job disappears with it. No external services, no message brokers, no dual-write risk.

Inspired by [RiverQueue](https://github.com/riverqueue/river) primarly and
[Asynq](https://github.com/hibiken/asynq). If you're using golang, RiverQueue is
great. 
Drip is basically a port of RiverQueue in clojure.

**Requires Java 21+.**

## Features

- Transactional job insertion — enqueue inside your own DB transactions
- Atomic job claiming
- Job priorities (1–4), scheduled execution, retries with exponential backoff
- Unique job constraints (by args, time period, queue)
- Per-kind retry policies and execution timeouts
- Queue pause/resume
- Periodic (fixed-interval) jobs
- Outbox pattern — complete jobs atomically with your own business writes
- Virtual threads (Java 21+)
- [Web UI](modules/drip-ui) — browse and manage jobs and queues in real time
- [CLI](modules/drip-cli) — command-line tool and interactive TUI for job management

<p align=center><img width="812" height="530"  alt="Screenshot 2026-05-04 at 20 53 26" src="https://github.com/user-attachments/assets/0a2805ed-bdce-40ca-b02a-debee7a377c5" /></p>

## Why transactional enqueueing?

Most job queues require two separate writes: one to your database, one to the queue. Between them, anything can go wrong — a crash, a network partition, a rollback — leaving your data and your queued work inconsistent.

Drip enqueues jobs in the same transaction as your business logic:

```clojure
;; Both the order row and the job are committed atomically.
;; If the transaction rolls back, neither exists.
(drip/with-tx [tx client]
  (insert-order! tx order-data)
  (drip/insert-job! client tx "send_confirmation" {:order-id order-id}))
```

Jobs are invisible to workers until the transaction commits. No polling delay, no phantom jobs from rolled-back writes. This eliminates a whole class of distributed systems problems without adding any infrastructure.

## Why a database-backed queue?

The conventional wisdom is that databases make poor queues — and for the wrong
workloads, that's true. But for most applications, it's the opposite of true.

**The "database queues don't scale" argument applies at a scale you probably
don't have.** A modern PostgreSQL or MariaDB instance handles thousands of job
inserts and claims per second without breaking a sweat. Dedicated brokers like
Redis or Kafka earn their complexity at hundreds of thousands of events per
second, or when you need cross-service fan-out, stream replay, or multi-consumer
topic semantics. Background jobs for most application are rarely in that
category.

What you get by staying in your database:

- **Zero new infrastructure.** No Redis, no RabbitMQ, no Kafka, no SQS. One
  fewer thing to operate, monitor, secure, back up, and pay for.
- **Transactional correctness for free.** Enqueue inside your existing
  transactions. The job exists if and only if your data exists. This is
  [genuinely hard to replicate](https://brandur.org/job-drain) with an
  external broker.
- **Familiar tooling.** `SELECT`, `EXPLAIN`, `psql`, your existing backups, your
  existing monitoring. No new query language, no new client library, no new
  failure modes.
- **SKIP LOCKED works.** PostgreSQL and MariaDB both support `SELECT ... FOR
  UPDATE SKIP LOCKED`, which makes concurrent job claiming efficient and
  contention-free. This is the key primitive that makes database queues
  practical at real throughput.
- **Operational simplicity.** The job table is just a table. You can query it,
  export it, restore it from a backup, and reason about it with standard SQL.

The trade-offs are real: you won't hit 1M jobs/sec on a single Postgres node,
and you don't get durable pub/sub fan-out to independent consumer groups. If you
need those things, use the right tool. But if you need reliable background job
processing with transactional guarantees and you're already running PostgreSQL
or MariaDB, adding a broker is pure overhead.

## Dependency

[![Clojars Project](https://img.shields.io/clojars/v/com.s-exp/drip.svg)](https://clojars.org/com.s-exp/drip)

Add the JDBC driver for your database:

```clojure
;; MariaDB 10.6+
org.mariadb.jdbc/mariadb-java-client {:mvn/version "3.3.3"}

;; PostgreSQL 9.5+
org.postgresql/postgresql {:mvn/version "42.7.3"}

;; SQLite 3.38.0+
org.xerial/sqlite-jdbc {:mvn/version "3.45.3.0"}
```

## Documentation

Full documentation is in the [`doc/`](doc/) directory:

- [Getting Started](doc/01-getting-started.md) — installation, client setup, migrations, lifecycle
- [Jobs](doc/02-jobs.md) — inserting, querying, updating, unique constraints, retries
- [Workers and the Executor](doc/03-workers.md) — registry, concurrency, retry policies, timeouts, retention
- [Queues](doc/04-queues.md) — pause/resume, multi-queue routing
- [Periodic Jobs](doc/05-periodic-jobs.md) — fixed-interval scheduling, deduplication
- [Patterns](doc/06-patterns.md) — transactional enqueue, outbox, fanout, chaining, testing
- [Schema Reference](doc/07-schema.md) — table definitions, indexes, dialect differences

## Quick start

```clojure
(require '[s-exp.drip :as drip]
         '[s-exp.drip.client.mariadb :as mariadb])
;; or: '[s-exp.drip.client.postgres :as postgres]
;; or: '[s-exp.drip.client.sqlite   :as sqlite]

;; 1. Create a client (wraps your DataSource)
(def client (mariadb/make-client datasource))

;; 2. Run migrations (idempotent, safe to call on every startup)
(drip/migrate! client)

;; 3. Start the executor
(def executor
  (drip/start-executor!
    {:client   client
     :registry {"send_email" (fn [_ job] (send-email! (:args job)))}
     :queues   ["default" "priority"]}))

;; 4. Insert a job (uses its own transaction)
(drip/insert-job client "send_email" {:to "user@example.com"})

;; 5. Stop on shutdown
(drip/stop-executor! executor)
```

## Transactional insertion

### Enqueue with your business writes

The most important pattern: insert a job in the same transaction as the data it depends on. Both succeed or both fail together.

```clojure
(drip/with-tx [tx client]
  (create-user! tx user-data)
  (drip/insert-job! client tx "send_welcome_email" {:user-id (:id user-data)}))
```

No welcome email queued for a user that was never created. No missing email for a user that was.

### Outbox pattern

Complete a job atomically with your own writes. Call `complete-job!` inside your transaction:

```clojure
{"process_payment" (fn [client job]
                     (drip/with-tx [tx client]
                       (record-payment! tx (:args job))        ; your write
                       (drip/complete-job! client tx (:id job))))} ; same tx
```

If the transaction rolls back, the job is not marked complete and will retry. If it commits, both the payment record and the job completion are durable.

### Batch insertion

Enqueue multiple jobs in a single transaction:

```clojure
;; Opens its own transaction
(drip/insert-many client
  [["send_email"  {:to "a@example.com"} nil]
   ["send_sms"    {:to "+1555..."} {:queue "sms"}]
   ["log_event"   {:type "signup"} {:priority 4}]])

;; Or within your own transaction
(drip/with-tx [tx client]
  (create-account! tx account-data)
  (drip/insert-many! client tx
    [["send_welcome"  {:user-id id} nil]
     ["provision_resources" {:account-id id} {:queue "infra"}]]))
```

## Concepts

### Job states

```
available → running → completed
                    → retryable → available (after backoff)
                                → discarded (max attempts reached)
                    → cancelled
scheduled → available (when scheduled_at <= now)
pending   → available | scheduled | cancelled
```

### Client

All operations take a client as the first argument. Create one with a dialect-specific constructor:

```clojure
(def client (mariadb/make-client datasource))
;; (def client (postgres/make-client datasource))
;; (def client (sqlite/make-client datasource))
```

### Inserting jobs

Each function has two variants: one that opens its own transaction (no `!` suffix), and one that takes an explicit transaction (`!` suffix):

```clojure
;; Uses client's own datasource
(drip/insert-job client "my_kind" {:key "value"})
(drip/insert-job client "my_kind" {:key "value"}
  :queue "priority"
  :priority 2
  :max-attempts 5
  :scheduled-at (-> (Instant/now) (.plusSeconds 3600))
  :tags ["important"]
  :metadata {:source "api"}
  :ephemeral true)   ; delete row immediately on success (no :completed record retained)

;; With an explicit transaction (atomic with your own writes)
(drip/with-tx [tx client]
  (create-user! tx user-data)
  (drip/insert-job! client tx "welcome_email" {:user-id (:id user)}
    :queue "default"))
```

Default insert opts: `queue="default"`, `priority=1`, `max-attempts=25`, `ephemeral=false`.

### Unique jobs

Prevent duplicate jobs using `:unique-opts`:

```clojure
(drip/insert-job client "report" {:period "daily"}
  :unique-opts
  {:by-args?  true                     ; distinct per args value
   :by-period "24h"                    ; one per 24h window
   :by-queue? true                     ; scope to queue
   :by-state  #{:available :pending :running :scheduled :retryable}})
```

A second insert within the same window throws a constraint violation. Periodic jobs use this automatically.

### Workers

Registry values are functions of two arguments: `[client job]`. Handlers must explicitly manage job state — call `complete-job!`, `snooze-job!`, etc. Throw any `Throwable` to signal failure — the executor records the error and schedules a retry (or discards when max attempts is reached).

```clojure
;; Minimal handler — call complete-job! explicitly
{"send_email" (fn [client job]
                (send-email! (:args job))
                (drip/complete-job client (:id job)))}

;; Atomic with DB writes
{"send_email" (fn [client job]
                (drip/with-tx [tx client]
                  (record-send! tx (:args job))
                  (drip/complete-job! client tx (:id job))))}

;; Close over dependencies
(defn email-worker [mailer]
  (fn [client job]
    (send-email! mailer (:args job))
    (drip/complete-job client (:id job))))

{"send_email" (email-worker mailer-instance)}
```

`(:args job)` contains the decoded args map. Other job fields: `:id`, `:kind`, `:queue`, `:state`, `:attempt`, `:max-attempts`, `:priority`, `:tags`, `:metadata`, `:scheduled-at`, `:created-at`, `:errors`.

### Job output

Persist a handler's result into the job's `metadata` column under the `"output"` key. Call `record-output!` before completing:

```clojure
{"compute_report" (fn [client job]
                    (let [result (run-report (:args job))]
                      (drip/with-tx [tx client]
                        (drip/record-output! client tx (:id job) result)
                        (drip/complete-job!  client tx (:id job)))))}

;; Retrieve later
(get-in (drip/get-job client job-id) [:metadata "output"])
```

### Executor options

```clojure
(drip/start-executor!
  {:client           client         ; required
   :registry         {...}          ; required, {kind-string (fn [client job] ...)}
   :queues           ["default"]    ; queues to consume (default ["default"])
   :concurrency      10             ; max simultaneous in-flight jobs (default 10)
   :poll-interval-ms 1000           ; polling interval in ms (default 1000)
   :worker-id        "my-worker-1"  ; unique ID (default: random UUID)
   :retry-policies   {:default  my-policy              ; default: exponential backoff (attempt^4 ± 10%)
                      "my_kind" fast-retry-policy}    ; per-kind overrides
   :job-timeouts     {:default  "30s"                 ; :default = global timeout; nil = no timeout
                      "slow_job" "2m"}              ; per-kind overrides; duration strings or ms
   :rescue-after     {:default "1h"  ; :default = global threshold (duration string or ms)
                      "slow" "4h"}  ; per-queue overrides; nil = disable rescue
   :retention        {:default  {:completed "24h"  ; :default = global retention windows
                                 :cancelled "24h"  ;   (nil = disable cleanup)
                                 :discarded "7d"}  ; duration strings or ms numbers accepted
                      "fast"    {:completed "1h"}  ; per-queue overrides merged with :default
                      "archive" {:discarded nil}}})
```

On PostgreSQL, a `LISTEN` connection starts automatically. Inserts in other processes trigger an immediate poll.

### Retry policies

Drip ships several built-in policy constructors. All take attempt (1-based long) and return a `java.time.Instant`.

```clojure
;; Default: attempt^4 seconds ± 10% jitter (25 attempts ≈ 4 days total)
drip/default-retry-policy

;; Always wait a fixed delay (duration string or raw ms number)
(drip/constant-retry-policy "30s")                       ; 30s
(drip/constant-retry-policy "30s" :jitter 0.1)           ; 30s ± 10% jitter
(drip/constant-retry-policy 30000)                       ; same, as raw ms

;; Delay grows linearly with attempt: base * attempt
(drip/linear-retry-policy "10s")                         ; 10s, 20s, 30s, …
(drip/linear-retry-policy "10s" :max "5m")               ; capped at 5m
(drip/linear-retry-policy "10s" :max "5m" :jitter 0.1)  ; + 10% jitter

;; Configurable exponential backoff: base * multiplier^(attempt-1)
(drip/exponential-retry-policy "1s")                                          ; 1s, 2s, 4s, … capped at 1h
(drip/exponential-retry-policy "1s" :multiplier 3.0)                          ; 1s, 3s, 9s, …
(drip/exponential-retry-policy "1s" :multiplier 2.0 :max "30m")               ; capped at 30m
(drip/exponential-retry-policy "1s" :multiplier 2.0 :max "30m" :jitter 0.15) ; + 15% jitter

;; Retry immediately — useful for tests or idempotent fast-fail jobs
(drip/immediate-retry-policy)

;; Custom: takes attempt (1-based long), returns java.time.Instant
(defn my-policy [attempt]
  (.plusSeconds (Instant/now) (* 30 attempt)))
```

Use `:retry-policies` with a `:default` key and per-kind overrides:

```clojure
(drip/start-executor!
  {:client  client
   :registry {"slow_job" slow-handler
              "fast_job" fast-handler}
   :retry-policies {:default  (drip/exponential-retry-policy "5s" :multiplier 2.0 :max "1h")
                    "fast_job" (drip/constant-retry-policy "2s")}})
```

### Queue management

```clojure
;; No-tx variants (use client's own datasource)
(drip/upsert-queue client "my-queue" {:description "My queue"})
(drip/pause-queue  client "my-queue")   ; workers stop fetching
(drip/resume-queue client "my-queue")
(drip/list-queues  client)

;; Explicit tx variants
(drip/upsert-queue! client tx "my-queue" {:description "My queue"})
(drip/pause-queue!  client tx "my-queue")
(drip/resume-queue! client tx "my-queue")
(drip/list-queues!  client tx)
```

### Periodic jobs

```clojure
(def scheduler
  (drip/start-periodic-executor! client
    [{:kind   "daily_report"
      :args   {:type "summary"}
      :period "24h"          ; ms number or string: "1h", "30m", "1d", etc.
      :queue  "default"
      :opts   nil}]))

;; On shutdown:
(drip/stop-periodic-executor! scheduler)
```

One job per period window — duplicate insertions are silently discarded.

### Job management

```clojure
;; No-tx variants (use client's own datasource)
(drip/get-job      client job-id)
(drip/cancel-job   client job-id)
(drip/retry-job    client job-id)    ; force back to :available
(drip/discard-job  client job-id)
(drip/snooze-job   client job-id "30m")  ; reschedule without consuming retry budget
(drip/complete-job client job-id)
(drip/record-output client job-id {:result 42})

;; Explicit tx variants
(drip/get-job!       client tx job-id)
(drip/cancel-job!    client tx job-id)
(drip/retry-job!     client tx job-id)
(drip/discard-job!   client tx job-id)
(drip/snooze-job!    client tx job-id "30m")
(drip/complete-job!  client tx job-id)
(drip/record-output! client tx job-id {:result 42})

;; Update job fields
(drip/update-job client job-id
  {:priority 3
   :queue "urgent"
   :metadata {:reason "reprioritized"}
   :max-attempts 10
   :tags ["vip"]
   :scheduled-at (Instant/now)})   ; also transitions state to :available/:scheduled

;; Swap — fetch, transform, update atomically
(drip/swap-job client job-id
  (fn [job] {:priority (max 1 (dec (:priority job)))}))

(drip/swap-job! client tx job-id
  (fn [job] {:metadata (assoc (:metadata job) "retried-by" "admin")}))

;; Fetch jobs (normally done by the executor)
(drip/fetch-jobs client "default" "worker-id" :limit 10)
(drip/fetch-jobs! client tx "default" "worker-id" {:limit 10})
```

### Listing jobs

```clojure
;; Single filters
(drip/list-jobs client {:state :running})
(drip/list-jobs client {:kind "send_email" :limit 50})

;; Multi-value OR filters
(drip/list-jobs client {:states [:running :retryable]
                         :kinds  ["send_email" "send_sms"]
                         :queues ["default" "priority"]
                         :priorities [1 2]})

;; Time range filters
(drip/list-jobs client {:created-after  (-> (Instant/now) (.minusSeconds 3600))
                         :created-before (Instant/now)
                         :scheduled-before (Instant/now)})

;; Cursor pagination (DESC by id)
(drip/list-jobs client {:limit 50})
;; next page:
(drip/list-jobs client {:limit 50 :after (:id (last page))})
```

### Deleting jobs

```clojure
;; Single job by ID
(drip/delete-job client job-id)

;; Bulk delete by criteria
(drip/delete-jobs client {:states [:completed :cancelled]
                            :finalized-before (.minusDays (Instant/now) 7)})

;; With additional filters
(drip/delete-jobs client {:states    [:discarded]
                            :kinds     ["old_job_type"]
                            :queues    ["legacy"]
                            :priorities [4]
                            :created-before (.minusDays (Instant/now) 30)})
```

## Schema

Migrations are managed by [migratus](https://github.com/yogthos/migratus). SQL files live in `resources/migrations/<dialect>/001-initial-schema.up.sql`. Key tables:

| Table | Purpose |
|---|---|
| `drip_job` | All jobs and their state |
| `drip_queue` | Queue metadata and pause state |
| `drip_migration` | Applied migration versions |

`migrate!` creates tables and tracks applied versions. Safe to call on every startup.

## Database notes

| Database | Min version | Notes |
|---|---|---|
| MariaDB | 10.6 | `FOR UPDATE SKIP LOCKED`, `JSON_ARRAY_APPEND` |
| PostgreSQL | 10 | `FOR UPDATE SKIP LOCKED`, `GENERATED ALWAYS AS IDENTITY`, `LISTEN`/`NOTIFY` on `drip_insert` |
| SQLite | 3.38.0 | No `SKIP LOCKED`; WAL mode; timestamps as ISO-8601 text; no notifications |

## Web UI

[drip-ui](modules/drip-ui) is an optional web dashboard for browsing and managing jobs and queues. It connects directly to your drip database via a JDBC URL — no extra infrastructure.

<img width="1624" height="1061" alt="Screenshot 2026-05-04 at 20 53 26" src="https://github.com/user-attachments/assets/27623b48-1329-47c2-8182-966f6bcd1e02" />


See [modules/drip-ui/README.md](modules/drip-ui/README.md) for full details.

## CLI

[drip-cli](modules/drip-cli) is a command-line tool for managing jobs and queues from the terminal. Supports PostgreSQL, MariaDB, and SQLite. Includes an interactive TUI browser with auto-refresh, job detail view, and keyboard navigation.

```bash
# List failed jobs
drip-cli --url 'jdbc:postgresql://localhost/mydb?user=pg&password=pg' jobs list --state failed

# Interactive TUI
drip-cli --url '...' jobs tui

# JSON output for scripting
drip-cli --url '...' jobs list --format json | jq '.[].id'
```

See [modules/drip-cli/README.md](modules/drip-cli/README.md) for full details.

## License

Copyright © 2026 Max Penet
Distributed under the Mozilla Public License Version 2.0
