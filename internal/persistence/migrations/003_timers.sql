CREATE TABLE IF NOT EXISTS timers (
    id              UUID PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    timer_name      TEXT NOT NULL,
    fire_at         TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    payload_json    JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fired_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_timers_due
    ON timers (fire_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_timers_run_id ON timers (run_id);
