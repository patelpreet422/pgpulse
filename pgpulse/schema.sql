-- PgPulse schema: High-Throughput Concurrent Job Queue for PostgreSQL
--
-- This migration creates the tables and indexes needed by PgPulse.
-- Run this once against your database before using the library.

CREATE TABLE IF NOT EXISTS pgpulse_jobs (
    id          BIGSERIAL    PRIMARY KEY,
    queue       TEXT         NOT NULL DEFAULT 'default',
    kind        TEXT         NOT NULL,
    payload     JSONB        NOT NULL DEFAULT '{}',
    priority    SMALLINT     NOT NULL DEFAULT 0,      -- higher = processed first
    state       TEXT         NOT NULL DEFAULT 'available',  -- available, running, completed, failed, discarded
    attempt     SMALLINT     NOT NULL DEFAULT 0,
    max_retries SMALLINT     NOT NULL DEFAULT 3,
    errors      JSONB,                                 -- array of error objects from past attempts
    scheduled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempted_at  TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,

    CONSTRAINT chk_state CHECK (state IN ('available','running','completed','failed','discarded'))
);

-- Index for the hot fetch path: find available jobs ordered by priority + scheduled time.
CREATE INDEX IF NOT EXISTS idx_pgpulse_jobs_fetchable
    ON pgpulse_jobs (queue, priority DESC, scheduled_at ASC)
    WHERE state = 'available';

-- Index for listing jobs by state.
CREATE INDEX IF NOT EXISTS idx_pgpulse_jobs_state
    ON pgpulse_jobs (state);
