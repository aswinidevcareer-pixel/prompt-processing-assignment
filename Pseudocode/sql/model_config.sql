
-- Live model configuration (hot-reloaded, zero downtime changes)
CREATE TABLE model_config (
    model_id        TEXT PRIMARY KEY,
    target_rpm      INTEGER NOT NULL DEFAULT 600,           -- requests per minute
    burst           INTEGER DEFAULT 200,                    -- maximum number of tokens in the bucket (burst capacity)
    weight          NUMERIC NOT NULL DEFAULT 1.0,           -- routing weight
    tier            TEXT NOT NULL DEFAULT 'standard',       -- premium, standard, cheap
    enabled         BOOLEAN DEFAULT true,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

