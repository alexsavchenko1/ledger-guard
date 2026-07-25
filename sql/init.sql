CREATE TABLE IF NOT EXISTS reconciliation_results (
    operation_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    events_count INTEGER NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
