-- This file is derived from Riverqueue
-- Licensed under the Mozilla Public License 2.0
-- https://mozilla.org/MPL/2.0/
--
-- Modifications have been made.

CREATE TYPE drip_job_state AS ENUM(
  'available',
  'cancelled',
  'completed',
  'discarded',
  'pending',
  'retryable',
  'running',
  'scheduled'
);

CREATE TABLE IF NOT EXISTS drip_queue (
    name       text         NOT NULL PRIMARY KEY,
    created_at timestamptz  NOT NULL DEFAULT NOW(),
    metadata   jsonb        NOT NULL DEFAULT '{}',
    paused_at  timestamptz  NULL,
    updated_at timestamptz  NOT NULL DEFAULT NOW(),
    CONSTRAINT name_length CHECK (char_length(name) > 0 AND char_length(name) < 128)
);

CREATE TABLE IF NOT EXISTS drip_job (
    id           bigint          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state        drip_job_state  NOT NULL DEFAULT 'available',
    attempt      smallint        NOT NULL DEFAULT 0,
    max_attempts smallint        NOT NULL DEFAULT 25,
    attempted_at timestamptz     NULL,
    created_at   timestamptz     NOT NULL DEFAULT NOW(),
    finalized_at timestamptz     NULL,
    scheduled_at timestamptz     NOT NULL DEFAULT NOW(),
    priority     smallint        NOT NULL DEFAULT 1,
    args         jsonb           NOT NULL DEFAULT '{}',
    attempted_by text[]          NOT NULL DEFAULT '{}',
    errors       jsonb[]         NOT NULL DEFAULT '{}',
    kind         text            NOT NULL,
    metadata     jsonb           NOT NULL DEFAULT '{}',
    queue        text            NOT NULL DEFAULT 'default',
    tags         varchar(255)[]  NOT NULL DEFAULT '{}',
    unique_key   bytea           NULL,
    unique_states BIT(8)         NULL,

    CONSTRAINT finalized_or_finalized_at_null
        CHECK ((state IN ('cancelled','completed','discarded') AND finalized_at IS NOT NULL)
            OR finalized_at IS NULL),
    CONSTRAINT max_attempts_is_positive CHECK (max_attempts > 0),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

-- Bitmask helper: checks if a state is active in the unique_states bitmask.
-- River bit layout (MSB=7 to LSB=0):
--   bit7=available, bit6=cancelled, bit5=completed, bit4=discarded,
--   bit3=pending,   bit2=retryable,  bit1=running,   bit0=scheduled
CREATE OR REPLACE FUNCTION drip_job_state_in_bitmask(bitmask BIT(8), state drip_job_state)
RETURNS boolean
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE state
        WHEN 'available' THEN get_bit(bitmask, 7)
        WHEN 'cancelled' THEN get_bit(bitmask, 6)
        WHEN 'completed' THEN get_bit(bitmask, 5)
        WHEN 'discarded' THEN get_bit(bitmask, 4)
        WHEN 'pending'   THEN get_bit(bitmask, 3)
        WHEN 'retryable' THEN get_bit(bitmask, 2)
        WHEN 'running'   THEN get_bit(bitmask, 1)
        WHEN 'scheduled' THEN get_bit(bitmask, 0)
        ELSE 0
    END = 1;
$$;

-- Primary fetch index: River order (state, queue, priority, scheduled_at, id)
CREATE INDEX IF NOT EXISTS drip_job_prioritized_fetching_index
    ON drip_job USING btree (state, queue, priority, scheduled_at, id);

CREATE INDEX IF NOT EXISTS drip_job_kind
    ON drip_job USING btree (kind);

CREATE INDEX IF NOT EXISTS drip_job_state_and_finalized_at_index
    ON drip_job USING btree (state, finalized_at)
    WHERE finalized_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS drip_job_args_index
    ON drip_job USING GIN (args);

CREATE INDEX IF NOT EXISTS drip_job_metadata_index
    ON drip_job USING GIN (metadata);

-- Partial unique index: only enforces uniqueness for states active in the bitmask.
CREATE UNIQUE INDEX IF NOT EXISTS drip_job_unique_idx
    ON drip_job (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND drip_job_state_in_bitmask(unique_states, state);
