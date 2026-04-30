CREATE TABLE IF NOT EXISTS batch_jobs (
    id              BIGSERIAL PRIMARY KEY,
    payload         JSONB NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'queued',
    locked_until    TIMESTAMP WITH TIME ZONE,
    checkpoint_data JSONB DEFAULT '{}',
    retries         INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Partial composite index: only covers rows that are actively polled.
-- This prevents index bloat from millions of completed/failed rows.
CREATE INDEX IF NOT EXISTS idx_batch_jobs_poll
    ON batch_jobs (status, locked_until) INCLUDE (id)
    WHERE status IN ('queued', 'processing');
