-- Drip: job queue schema for SQLite
-- Migration version: 1
-- Requires: SQLite 3.38.0+ (for json_insert / json_array functions)
-- Timestamps stored as ISO-8601 TEXT in UTC: 'YYYY-MM-DDTHH:MM:SS.sssZ'

CREATE TABLE IF NOT EXISTS drip_queue (
    name       TEXT    NOT NULL PRIMARY KEY,
    created_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    metadata   TEXT    NOT NULL DEFAULT '{}',
    paused_at  TEXT    NULL,
    updated_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
--;;
-- Job states: available, cancelled, completed, discarded,
--             pending, retryable, running, scheduled
-- Priority: 1 (highest) to 4 (lowest)
-- encoded_args: JSON-encoded job arguments (BLOB, unlimited size)
-- attempted_by: JSON array of worker-id strings (TEXT)
-- errors: JSON array of {error, trace, at} objects (TEXT)
-- unique_key: SHA-256 hex for deduplication (BLOB, 32 bytes)
-- unique_states: bitmask of states where uniqueness is enforced (INTEGER)
CREATE TABLE IF NOT EXISTS drip_job (
    id            INTEGER  PRIMARY KEY AUTOINCREMENT,
    attempt       INTEGER  NOT NULL DEFAULT 0,
    attempted_at  TEXT     NULL,
    attempted_by  TEXT     NOT NULL DEFAULT '[]',
    created_at    TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    encoded_args  BLOB     NOT NULL,
    errors        TEXT     NOT NULL DEFAULT '[]',
    finalized_at  TEXT     NULL,
    kind          TEXT     NOT NULL,
    max_attempts  INTEGER  NOT NULL DEFAULT 25,
    metadata      TEXT     NOT NULL DEFAULT '{}',
    priority      INTEGER  NOT NULL DEFAULT 1,
    queue         TEXT     NOT NULL DEFAULT 'default',
    scheduled_at  TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    state         TEXT     NOT NULL DEFAULT 'available',
    tags          TEXT     NOT NULL DEFAULT '[]',
    unique_key    BLOB     NULL,
    unique_states INTEGER  NULL,
    CONSTRAINT chk_drip_job_state
        CHECK (state IN ('available','cancelled','completed','discarded',
                         'pending','retryable','running','scheduled')),
    CONSTRAINT chk_drip_job_priority
        CHECK (priority BETWEEN 1 AND 4)
);
--;;
-- Primary fetch index: queue + state + priority + scheduled_at
CREATE INDEX IF NOT EXISTS idx_drip_job_fetch
    ON drip_job (queue, state, priority, scheduled_at);
--;;
-- Unique job deduplication index.
-- NULL unique_key rows are exempt (SQLite NULL != NULL in UNIQUE indexes).
CREATE UNIQUE INDEX IF NOT EXISTS uq_drip_job_unique_key
    ON drip_job (unique_key);
