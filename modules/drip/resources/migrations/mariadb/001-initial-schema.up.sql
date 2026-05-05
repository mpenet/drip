-- Drip: job queue schema for MariaDB
-- Migration version: 1
-- Requires: MariaDB 10.6+ (for SELECT ... FOR UPDATE SKIP LOCKED)

CREATE TABLE IF NOT EXISTS drip_queue (
    name       VARCHAR(128) NOT NULL PRIMARY KEY,
    created_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    metadata   JSON         NOT NULL DEFAULT (JSON_OBJECT()),
    paused_at  DATETIME(6)  NULL,
    updated_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
--;;
-- Job states: available, cancelled, completed, discarded,
--             pending, retryable, running, scheduled
-- Priority: 1 (highest) to 4 (lowest)
-- encoded_args: JSON-encoded job arguments (MEDIUMBLOB, up to 16MB)
-- attempted_by: JSON array of worker-id strings
-- errors: JSON array of {error, trace, at} objects
-- unique_key: SHA-256(kind[+args][+period][+queue]) for deduplication
-- unique_states: bitmask of states where uniqueness is enforced
--   bit 0=available, 1=cancelled, 2=completed, 3=discarded,
--   bit 4=pending,   5=retryable,  6=running,   7=scheduled
CREATE TABLE IF NOT EXISTS drip_job (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    attempt       INT           NOT NULL DEFAULT 0,
    attempted_at  DATETIME(6)   NULL,
    attempted_by  JSON          NOT NULL DEFAULT (JSON_ARRAY()),
    created_at    DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    encoded_args  MEDIUMBLOB    NOT NULL,
    errors        JSON          NOT NULL DEFAULT (JSON_ARRAY()),
    finalized_at  DATETIME(6)   NULL,
    kind          VARCHAR(128)  NOT NULL,
    max_attempts  INT           NOT NULL DEFAULT 25,
    metadata      JSON          NOT NULL DEFAULT (JSON_OBJECT()),
    priority      TINYINT       NOT NULL DEFAULT 1,
    queue         VARCHAR(128)  NOT NULL DEFAULT 'default',
    scheduled_at  DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    state         VARCHAR(20)   NOT NULL DEFAULT 'available',
    tags          JSON          NOT NULL DEFAULT (JSON_ARRAY()),
    unique_key    VARBINARY(32) NULL,
    unique_states SMALLINT      NULL,
    CONSTRAINT chk_drip_job_state
        CHECK (state IN ('available','cancelled','completed','discarded',
                         'pending','retryable','running','scheduled')),
    CONSTRAINT chk_drip_job_priority
        CHECK (priority BETWEEN 1 AND 4)
) ENGINE=InnoDB;
--;;
-- Primary fetch index: queue + state + priority + scheduled_at
-- Used by: SELECT ... WHERE queue=? AND state='available' AND scheduled_at<=?
--          ORDER BY priority ASC, scheduled_at ASC ... FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_drip_job_fetch
    ON drip_job (queue, state, priority, scheduled_at);
--;;
-- Unique job deduplication index.
-- NULL unique_key rows are exempt (NULL != NULL in SQL UNIQUE constraints),
-- so non-unique jobs coexist freely.
CREATE UNIQUE INDEX IF NOT EXISTS uq_drip_job_unique_key
    ON drip_job (unique_key);
