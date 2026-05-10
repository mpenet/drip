# Drip Documentation

## Contents

1. [Getting Started](01-getting-started.md) — Installation, client setup, migrations, lifecycle
2. [Jobs](02-jobs.md) — Inserting, querying, updating, unique constraints, retries
3. [Workers](03-workers.md) — Registry, concurrency, retry policies, timeouts, retention
4. [Queues](04-queues.md) — Pause/resume, multi-queue routing, queue records
5. [Periodic Jobs](05-periodic-jobs.md) — Fixed-interval scheduling, deduplication, lifecycle
6. [Patterns](06-patterns.md) — Transactional enqueue, outbox, fanout, chaining, testing
7. [Schema Reference](07-schema.md) — Table definitions, indexes, dialect differences

## Quick navigation

| I want to… | See |
|---|---|
| Get something running fast | [Getting Started](01-getting-started.md) |
| Insert a job in my own transaction | [Jobs → Explicit transaction variant](02-jobs.md#explicit-transaction-variant) |
| Understand job states | [Jobs → Job states](02-jobs.md#job-states) |
| Prevent duplicate jobs | [Jobs → Unique jobs](02-jobs.md#unique-jobs) |
| Configure retries | [Workers → Retry policies](03-workers.md#retry-policies) |
| Set job timeouts | [Workers → Job timeouts](03-workers.md#job-timeouts) |
| Pause a queue | [Queues → Pausing and resuming](04-queues.md#pausing-and-resuming) |
| Schedule recurring work | [Periodic Jobs](05-periodic-jobs.md) |
| Commit job completion with my own DB writes | [Patterns → Outbox pattern](06-patterns.md#outbox-pattern) |
| Test my job handlers | [Patterns → Testing](06-patterns.md#testing) |
| Understand the database tables | [Schema Reference](07-schema.md) |
