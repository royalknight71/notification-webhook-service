DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'webhook_events'
          AND column_name = 'payload'
          AND data_type <> 'bytea'
    ) THEN
        ALTER TABLE webhook_events
            ALTER COLUMN payload TYPE BYTEA
            USING convert_to(payload::text, 'UTF8');
    END IF;
END
$$;
