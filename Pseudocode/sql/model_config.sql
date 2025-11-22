
-- Live model configuration (hot-reloaded, zero downtime changes)
CREATE TABLE model_config (
    model_id        TEXT PRIMARY KEY,
    target_rpm      INTEGER NOT NULL DEFAULT 600,           -- requests per minute
    weight          NUMERIC NOT NULL DEFAULT 1.0,           -- routing weight
    tier            TEXT NOT NULL DEFAULT 'standard',       -- premium, standard, cheap
    burst           INTEGER DEFAULT 200,
    enabled         BOOLEAN DEFAULT true,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
-- Example data
INSERT INTO model_config (model_id, target_rpm, weight, tier) VALUES
('gpt-4o',        1200, 10.0, 'premium'),
('claude-3.5-sonnet', 1000, 8.0,  'premium'),
('gpt-4o-mini',   3600, 5.0,  'standard'),
