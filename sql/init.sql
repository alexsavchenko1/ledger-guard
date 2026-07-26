CREATE TABLE IF NOT EXISTS reconciliation_results (
    operation_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    events_count INTEGER NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS operation_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    client_id TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    currency TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (
        event_id,
        operation_id,
        event_type,
        source,
        client_id,
        amount,
        currency,
        occurred_at
    )
);

CREATE INDEX IF NOT EXISTS idx_operation_events_operation_id
    ON operation_events (operation_id);
