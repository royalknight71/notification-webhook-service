CREATE TABLE IF NOT EXISTS webhook_events (
    id           TEXT PRIMARY KEY,
    target_url   TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS deliveries (
    event_id      TEXT PRIMARY KEY REFERENCES webhook_events(id) ON DELETE CASCADE,
    state         TEXT NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending', 'processing', 'delivered', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ,
    claim_until   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS deliveries_state_updated_idx
    ON deliveries (state, updated_at);

CREATE TABLE IF NOT EXISTS delivery_outbox (
    event_id      TEXT PRIMARY KEY REFERENCES webhook_events(id) ON DELETE CASCADE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,
    claim_token   UUID,
    claim_until   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS delivery_outbox_pending_idx
    ON delivery_outbox (created_at) WHERE published_at IS NULL;
