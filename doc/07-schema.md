# Schema Reference

## Tables

Drip creates three tables, all prefixed with `drip_`:

| Table | Purpose |
|---|---|
| `drip_job` | All jobs and their current state |
| `drip_queue` | Queue metadata and pause state |
| `drip_migration` | Applied migration version tracking |

## drip_job

The central table. Each row is one job.

| Column | Type (PG) | Type (MariaDB) | Type (SQLite) | Description |
|---|---|---|---|---|
| `id` | `bigint GENERATED ALWAYS AS IDENTITY` | `BIGINT AUTO_INCREMENT` | `INTEGER` | Primary key |
| `state` | `drip_job_state` (ENUM) | `VARCHAR(20)` + CHECK | `TEXT` + CHECK | Job state keyword |
| `attempt` | `smallint` | `INT` | `INTEGER` | Times attempted (0 on insert) |
| `max_attempts` | `smallint` | `INT` | `INTEGER` | Max before discard (default 25) |
| `attempted_at` | `timestamptz` | `DATETIME(6)` | `TEXT` (ISO-8601) | When last attempt started |
| `created_at` | `timestamptz` | `DATETIME(6)` | `TEXT` (ISO-8601) | Insert time |
| `finalized_at` | `timestamptz` | `DATETIME(6)` | `TEXT` (ISO-8601) | When reached terminal state |
| `scheduled_at` | `timestamptz` | `DATETIME(6)` | `TEXT` (ISO-8601) | Earliest eligible run time |
| `priority` | `smallint` | `TINYINT` | `INTEGER` | 1 (highest) – 4 (lowest) |
| `args` | `jsonb` | `MEDIUMBLOB` (JSON bytes) | `TEXT` (JSON) | Handler arguments |
| `attempted_by` | `text[]` | `JSON` (array) | `TEXT` (JSON array) | Worker IDs that attempted this job |
| `errors` | `jsonb[]` | `JSON` (array) | `TEXT` (JSON array) | Error history `[{error, trace, at}]` |
| `kind` | `text` | `VARCHAR(128)` | `TEXT` | Job type string |
| `metadata` | `jsonb` | `JSON` | `TEXT` (JSON) | User-controlled key/value store |
| `queue` | `text` | `VARCHAR(128)` | `TEXT` | Queue name |
| `tags` | `varchar(255)[]` | `JSON` (array) | `TEXT` (JSON array) | String labels |
| `unique_key` | `bytea` | `VARBINARY(32)` | `BLOB` | SHA-256 dedup key (NULL = no constraint) |
| `unique_states` | `BIT(8)` | `SMALLINT` | `INTEGER` | Bitmask of states where uniqueness is enforced |

### Constraints (PostgreSQL)

```sql
-- Terminal states must have finalized_at
CONSTRAINT finalized_or_finalized_at_null
    CHECK ((state IN ('cancelled','completed','discarded') AND finalized_at IS NOT NULL)
        OR finalized_at IS NULL)

CONSTRAINT max_attempts_is_positive CHECK (max_attempts > 0)
CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4)
CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128)
CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
```

### Indexes (PostgreSQL)

```sql
-- Primary fetch index: covers the FOR UPDATE SKIP LOCKED query
-- (state, queue, priority, scheduled_at, id)
drip_job_prioritized_fetching_index

-- Kind lookups
drip_job_kind

-- Retention/cleanup queries
drip_job_state_and_finalized_at_index  (partial: WHERE finalized_at IS NOT NULL)

-- JSON search
drip_job_args_index      (GIN on args)
drip_job_metadata_index  (GIN on metadata)

-- Unique job deduplication (partial: WHERE unique_key IS NOT NULL AND ...)
drip_job_unique_idx
```

MariaDB uses a composite `btree` index on `(queue, state, priority, scheduled_at)`. SQLite uses the same composite index. Neither supports partial indexes or GIN.

### State bitmask

`unique_states` encodes which states enforce uniqueness using River's bit layout:

```
bit7=available  bit6=cancelled  bit5=completed  bit4=discarded
bit3=pending    bit2=retryable  bit1=running    bit0=scheduled
```

PostgreSQL uses a `BIT(8)` column and a helper function `drip_job_state_in_bitmask` in the partial unique index predicate. MariaDB and SQLite store the bitmask as a plain integer and evaluate it in the application layer.

## drip_queue

| Column | Type | Description |
|---|---|---|
| `name` | text / VARCHAR(128) | Primary key, queue name |
| `created_at` | timestamptz / DATETIME(6) / TEXT | First upsert time |
| `updated_at` | timestamptz / DATETIME(6) / TEXT | Last modification time |
| `paused_at` | timestamptz / DATETIME(6) / TEXT (nullable) | Set when paused; NULL when running |
| `metadata` | jsonb / JSON / TEXT | User-supplied metadata |

## drip_migration

| Column | Type | Description |
|---|---|---|
| `id` | bigint / BIGINT / INTEGER | Primary key |
| `created_at` | timestamptz / DATETIME(6) / TEXT | When the migration was applied |
| `version` | bigint / BIGINT / INTEGER | Migration version number (unique) |

Managed by [migratus](https://github.com/yogthos/migratus). `migrate!` records each applied version here; already-applied versions are skipped on subsequent startups. Currently only version `1` exists.

## PostgreSQL-specific: LISTEN/NOTIFY

The PostgreSQL migration does not create a trigger. Instead, `insert-job!` calls `pg_notify('drip_insert', queue)` after inserting. The executor's listener thread runs `LISTEN drip_insert` and calls `poll-fn` immediately on notification.

## Dialect differences

| Feature | PostgreSQL | MariaDB | SQLite |
|---|---|---|---|
| State type | `drip_job_state` ENUM | `VARCHAR(20)` + CHECK | `TEXT` + CHECK |
| args/errors/tags storage | `jsonb` / `jsonb[]` / `text[]` | `MEDIUMBLOB` / `JSON` / `JSON` | `TEXT` (JSON string) |
| Timestamps | `timestamptz` (with tz) | `DATETIME(6)` (UTC by convention) | `TEXT` ISO-8601 |
| unique_key | `bytea` | `VARBINARY(32)` | `BLOB` |
| Partial unique index | Yes (`WHERE unique_key IS NOT NULL AND drip_job_state_in_bitmask(...)`) | No (NULL-safe UNIQUE; bitmask evaluated in app) | No (same as MariaDB) |
| GIN indexes | Yes (args, metadata) | No | No |
| SKIP LOCKED | Yes (10+) | Yes (10.6+) | No (serialized via WAL) |
| LISTEN/NOTIFY | Yes (`drip_insert` channel) | No (polling only) | No (polling only) |
| Auto-increment | `GENERATED ALWAYS AS IDENTITY` | `AUTO_INCREMENT` | SQLite rowid |

## Migration files

SQL migration files (migratus format) are bundled in the Drip jar:

```
resources/migrations/mariadb/001-initial-schema.up.sql
resources/migrations/postgres/001-initial-schema.up.sql
resources/migrations/sqlite/001-initial-schema.up.sql
```

`migrate!` delegates to [migratus](https://github.com/yogthos/migratus) and applies pending migrations in version order. The tracking table is named `drip_migration`. To inspect the DDL for your dialect, extract from the jar or browse the source repository.

## Direct SQL access

Since jobs are plain rows, you can query them directly from any SQL tool:

```sql
-- Jobs waiting to run
SELECT id, kind, queue, priority, scheduled_at, attempt
FROM drip_job
WHERE state = 'available'
ORDER BY priority ASC, scheduled_at ASC;

-- Error history for a job
SELECT id, kind, errors
FROM drip_job
WHERE id = 12345;

-- Retry rate by kind (last 24 hours)
SELECT kind,
       COUNT(*) FILTER (WHERE state = 'retryable') AS retrying,
       COUNT(*) FILTER (WHERE state = 'discarded') AS dead,
       COUNT(*) AS total
FROM drip_job
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY kind;

-- Average queue depth by queue
SELECT queue, COUNT(*) AS depth
FROM drip_job
WHERE state = 'available'
GROUP BY queue;
```

Drip does not own these tables exclusively — you can add your own indexes, build dashboards on top, or write monitoring queries without affecting Drip's operation.
