ALTER TABLE webhook_events
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS webhook_events_idempotency_key_idx
    ON webhook_events (idempotency_key);
