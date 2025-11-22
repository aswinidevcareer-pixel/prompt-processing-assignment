
-- Dead Letter Queue tracking (what went to tasks.dlq)
CREATE TABLE dlq_events (
    id              BIGSERIAL PRIMARY KEY,
    task_id         BIGINT REFERENCES tasks(id),
    error           TEXT NOT NULL,
    dlq_at          TIMESTAMPTZ DEFAULT NOW(),
    model           TEXT NOT NULL
);
CREATE INDEX ix_dlq_events_resolved ON dlq_events(resolved);
CREATE INDEX ix_dlq_events_dlq_at ON dlq_events(dlq_at DESC);
