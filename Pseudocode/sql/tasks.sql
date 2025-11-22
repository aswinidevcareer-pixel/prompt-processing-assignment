-- 1. Main tasks table (source of truth)
CREATE TABLE tasks (
    id              BIGSERIAL PRIMARY KEY,
    prompt          TEXT NOT NULL,
    answer          TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',      -- pending, solved, failed
    priority        TEXT NOT NULL DEFAULT 'cold',         -- hot, cold
    created_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    solved_at       TIMESTAMPTZ
);
CREATE INDEX ix_tasks_status_priority ON tasks(status, priority);
CREATE INDEX ix_tasks_created_at ON tasks(created_at DESC);
