package delivery

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Requires TEST_DATABASE_URL pointing to a disposable PostgreSQL database.
func postgresIntegrationStore(t *testing.T) (context.Context, *pgxpool.Pool, *Store) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := "delivery_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, `DROP SCHEMA `+schema+` CASCADE`)
	})
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := NewStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, pool, store
}

func TestStorePostgresIntegration(t *testing.T) {
	ctx, _, store := postgresIntegrationStore(t)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	payload := []byte("{\n  \"z\": 1.00,\n  \"a\": 2e+03\n}")
	event := Event{ID: uuid.NewString(), TargetURL: "https://example.test/hook", Payload: payload}
	inserted, err := store.Accept(ctx, event)
	if err != nil || !inserted {
		t.Fatalf("first accept inserted=%v err=%v", inserted, err)
	}
	inserted, err = store.Accept(ctx, event)
	if err != nil || inserted {
		t.Fatalf("duplicate accept inserted=%v err=%v", inserted, err)
	}
	if pending, err := store.PendingOutboxCount(ctx); err != nil || pending != 1 {
		t.Fatalf("pending outbox count=%d err=%v, want 1", pending, err)
	}
	conflict := event
	conflict.TargetURL = "https://different.test/"
	if _, err := store.Accept(ctx, conflict); err != ErrIDConflict {
		t.Fatalf("expected ID conflict, got %v", err)
	}
	items, err := store.ClaimOutbox(ctx, 10, time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("claim outbox got %d items, err=%v", len(items), err)
	}
	if items[0].ID != event.ID || items[0].ClaimToken == "" {
		t.Fatalf("unexpected outbox claim: %+v", items[0])
	}
	if string(items[0].Payload) != string(payload) {
		t.Fatalf("payload bytes changed in PostgreSQL: got %q, want %q", items[0].Payload, payload)
	}
	claimToken := items[0].ClaimToken
	items, err = store.ClaimOutbox(ctx, 10, time.Minute)
	if err != nil || len(items) != 0 {
		t.Fatalf("duplicate outbox claim got %d items, err=%v", len(items), err)
	}
	if err := store.MarkPublished(ctx, event.ID, claimToken); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.PendingOutboxCount(ctx); err != nil || pending != 0 {
		t.Fatalf("pending outbox count after publish=%d err=%v, want 0", pending, err)
	}
	claim, attempts, err := store.ClaimDelivery(ctx, event.ID, time.Minute)
	if err != nil || claim != Claimed || attempts != 0 {
		t.Fatalf("claim delivery result=%v attempts=%d err=%v", claim, attempts, err)
	}
	if n, err := store.RecordAttempt(ctx, event.ID); err != nil || n != 1 {
		t.Fatalf("record attempt n=%d err=%v", n, err)
	}
	if err := store.RecordAttemptError(ctx, event.ID, "temporary", false); err != nil {
		t.Fatal(err)
	}
	claim, _, err = store.ClaimDelivery(ctx, event.ID, time.Minute)
	if err != nil || claim != ClaimBusy {
		t.Fatalf("expected active claim busy, got %v err=%v", claim, err)
	}
	if err := store.ReleaseDelivery(ctx, event.ID); err != nil {
		t.Fatal(err)
	}
	claim, attempts, err = store.ClaimDelivery(ctx, event.ID, time.Minute)
	if err != nil || claim != Claimed || attempts != 1 {
		t.Fatalf("reclaim result=%v attempts=%d err=%v", claim, attempts, err)
	}
	if n, err := store.RecordAttempt(ctx, event.ID); err != nil || n != 2 {
		t.Fatalf("record second attempt n=%d err=%v", n, err)
	}
	if err := store.RecordAttemptError(ctx, event.ID, "permanent", true); err != nil {
		t.Fatal(err)
	}
	claim, _, err = store.ClaimDelivery(ctx, event.ID, time.Minute)
	if err != nil || claim != ClaimFailed {
		t.Fatalf("failed delivery should be claimable for dead-letter routing; got %v err=%v", claim, err)
	}
	claim, _, err = store.ClaimDelivery(ctx, uuid.NewString(), time.Minute)
	if err != nil || claim != ClaimNotFound {
		t.Fatalf("unknown delivery should be distinguished; got %v err=%v", claim, err)
	}
	state, err := store.GetDeliveryState(ctx, event.ID)
	if err != nil || state != "failed" {
		t.Fatalf("state=%q err=%v", state, err)
	}

	second := Event{ID: uuid.NewString(), TargetURL: event.TargetURL, Payload: event.Payload}
	if _, err := store.Accept(ctx, second); err != nil {
		t.Fatal(err)
	}
	claim, _, err = store.ClaimDelivery(ctx, second.ID, time.Minute)
	if err != nil || claim != Claimed {
		t.Fatalf("second claim=%v err=%v", claim, err)
	}
	if err := store.MarkDelivered(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
	state, err = store.GetDeliveryState(ctx, second.ID)
	if err != nil || state != "delivered" {
		t.Fatalf("delivered state=%q err=%v", state, err)
	}
}

func TestStorePostgresAcceptRollsBackAtomically(t *testing.T) {
	ctx, pool, store := postgresIntegrationStore(t)
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION reject_outbox_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER reject_outbox_insert BEFORE INSERT ON delivery_outbox FOR EACH ROW EXECUTE FUNCTION reject_outbox_insert()`); err != nil {
		t.Fatal(err)
	}

	event := Event{ID: uuid.NewString(), TargetURL: "https://example.test", Payload: []byte(`{"ok":true}`)}
	if inserted, err := store.Accept(ctx, event); err == nil || inserted {
		t.Fatalf("expected injected outbox failure and transaction rollback; inserted=%v err=%v", inserted, err)
	}
	var events, deliveries, outbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM webhook_events WHERE id=$1`, event.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE event_id=$1`, event.ID).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_outbox WHERE event_id=$1`, event.ID).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if events != 0 || deliveries != 0 || outbox != 0 {
		t.Fatalf("partial event transaction remains: events=%d deliveries=%d outbox=%d", events, deliveries, outbox)
	}
}

func TestStorePostgresConcurrentOutboxClaimsAndLeaseRecovery(t *testing.T) {
	ctx, pool, store := postgresIntegrationStore(t)
	event := Event{ID: uuid.NewString(), TargetURL: "https://example.test", Payload: []byte(`null`)}
	if inserted, err := store.Accept(ctx, event); err != nil || !inserted {
		t.Fatalf("accept event inserted=%v err=%v", inserted, err)
	}

	type claimResult struct {
		items []OutboxItem
		err   error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			items, err := store.ClaimOutbox(ctx, 1, time.Minute)
			results <- claimResult{items: items, err: err}
		}()
	}
	close(start)
	var first OutboxItem
	claimed := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if len(result.items) > 1 {
			t.Fatalf("worker claimed %d rows, expected at most one", len(result.items))
		}
		if len(result.items) == 1 {
			claimed++
			first = result.items[0]
		}
	}
	if claimed != 1 || first.ID != event.ID {
		t.Fatalf("active outbox row was not exclusively claimed: claimed=%d item=%+v", claimed, first)
	}

	if _, err := pool.Exec(ctx, `UPDATE delivery_outbox SET claim_until=now()-interval '1 second' WHERE event_id=$1`, event.ID); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.ClaimOutbox(ctx, 1, time.Minute)
	if err != nil || len(recovered) != 1 || recovered[0].ID != event.ID {
		t.Fatalf("expired lease was not recovered: items=%+v err=%v", recovered, err)
	}
	if recovered[0].ClaimToken == first.ClaimToken {
		t.Fatal("lease recovery reused the previous claim token")
	}
	if err := store.MarkPublished(ctx, event.ID, first.ClaimToken); err == nil {
		t.Fatal("stale claim token unexpectedly marked the outbox row published")
	}
	if err := store.MarkPublished(ctx, event.ID, recovered[0].ClaimToken); err != nil {
		t.Fatalf("mark recovered outbox row published: %v", err)
	}
}

func TestStorePostgresMigratesLegacyJSONBPayload(t *testing.T) {
	ctx, pool, store := postgresIntegrationStore(t)
	if _, err := pool.Exec(ctx, `ALTER TABLE webhook_events ALTER COLUMN payload TYPE JSONB USING convert_from(payload, 'UTF8')::jsonb`); err != nil {
		t.Fatal(err)
	}
	legacyID := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO webhook_events(id,target_url,payload) VALUES($1,$2,$3::jsonb)`, legacyID, "https://example.test", `{"legacy":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version='002_payload_bytes.sql'`); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("apply legacy payload conversion migration: %v", err)
	}
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM webhook_events WHERE id=$1`, legacyID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"legacy": 1}` {
		t.Fatalf("legacy JSONB payload conversion got %q", payload)
	}
}
