CREATE TABLE IF NOT EXISTS workflow_runs (
    id              UUID PRIMARY KEY,
    namespace       TEXT NOT NULL,
    workflow_name   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',
    input_json      JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_ns_status
    ON workflow_runs (namespace, status, created_at DESC);

CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    event_type      TEXT NOT NULL,
    payload_json    JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_run_id ON events (run_id, id);

CREATE TABLE IF NOT EXISTS tasks (
    id              UUID PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    task_type       TEXT NOT NULL,
    activity_name   TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    payload_json    JSONB NOT NULL DEFAULT '{}',
    attempt         INT NOT NULL DEFAULT 1,
    max_attempts    INT NOT NULL DEFAULT 5,
    lease_owner     TEXT,
    lease_expires_at TIMESTAMPTZ,
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_poll
    ON tasks (status, scheduled_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_tasks_run_id ON tasks (run_id);
