package delivery

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Requires TEST_DATABASE_URL pointing to a disposable PostgreSQL database.
func TestStorePostgresIntegration(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "delivery_test_" + uuid.NewString()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := NewStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
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
	if err != nil || claim != ClaimTerminal {
		t.Fatalf("failed delivery should be terminal; got %v err=%v", claim, err)
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
