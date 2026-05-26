ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS task_queue TEXT NOT NULL DEFAULT 'default';

CREATE INDEX IF NOT EXISTS idx_tasks_poll_queue
    ON tasks (task_queue, status, scheduled_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_tasks_expired_lease
    ON tasks (lease_expires_at)
    WHERE status = 'leased';

CREATE TABLE IF NOT EXISTS task_completions (
    task_id           UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    idempotency_key   TEXT NOT NULL,
    result_json       JSONB NOT NULL DEFAULT '{}',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (task_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS task_failures (
    task_id           UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    idempotency_key   TEXT NOT NULL,
    error_message     TEXT NOT NULL,
    will_retry        BOOLEAN NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (task_id, idempotency_key)
);
