package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"notification-webhook-service/internal/observability"
	"notification-webhook-service/internal/queue"
)

type OutboxStore interface {
	ClaimOutbox(context.Context, int, time.Duration) ([]OutboxItem, error)
	MarkPublished(context.Context, string, string) error
	ReleaseOutbox(context.Context, string, string) error
}

// StartDispatcher continuously publishes committed outbox rows. Claims are leased,
// so a process crash releases work automatically without holding DB transactions
// across RabbitMQ network operations.
func StartDispatcher(ctx context.Context, store OutboxStore, publisher queue.Publisher) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			dispatchBatch(ctx, store, publisher)
			if health, ok := publisher.(queue.HealthReporter); ok && !health.Healthy() {
				slog.Error("outbox dispatcher stopped because publisher is unhealthy")
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

func dispatchBatch(ctx context.Context, store OutboxStore, publisher queue.Publisher) {
	// This dispatcher publishes serially, so claim only the row it can publish
	// within the lease. A single row keeps 30 seconds safely above the 10 second
	// publish confirmation timeout plus the bounded published-state update.
	items, err := store.ClaimOutbox(ctx, 1, 30*time.Second)
	if err != nil {
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "outbox claim failed", "error", observability.ErrorSummary(err))
		}
		return
	}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if releaseErr := store.ReleaseOutbox(releaseCtx, item.ID, item.ClaimToken); releaseErr != nil {
				slog.Error("outbox claim release failed during shutdown", "event_id", item.ID, "error", observability.ErrorSummary(releaseErr))
			}
			cancel()
			return
		}
		body, err := marshalEvent(item.Event)
		if err == nil {
			err = publisher.Publish(ctx, item.ID, body)
		}
		if err != nil {
			slog.ErrorContext(ctx, "outbox publish failed", "event_id", item.ID, "error", observability.ErrorSummary(err))
			releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if releaseErr := store.ReleaseOutbox(releaseCtx, item.ID, item.ClaimToken); releaseErr != nil {
				slog.Error("outbox claim release failed", "event_id", item.ID, "error", observability.ErrorSummary(releaseErr))
			}
			cancel()
			continue
		}
		markCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := store.MarkPublished(markCtx, item.ID, item.ClaimToken); err != nil {
			slog.ErrorContext(ctx, "outbox publish state update failed", "event_id", item.ID, "error", observability.ErrorSummary(err))
		} else {
			slog.InfoContext(ctx, "outbox event published", "event_id", item.ID)
		}
		cancel()
	}
}

func marshalEvent(ev Event) ([]byte, error) {
	if !json.Valid(ev.Payload) {
		return nil, fmt.Errorf("event %s has invalid JSON payload", ev.ID)
	}
	id, err := json.Marshal(ev.ID)
	if err != nil {
		return nil, err
	}
	targetURL, err := json.Marshal(ev.TargetURL)
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	body.Grow(len(id) + len(targetURL) + len(ev.Payload) + 29)
	body.WriteString(`{"id":`)
	body.Write(id)
	body.WriteString(`,"target_url":`)
	body.Write(targetURL)
	body.WriteString(`,"payload":`)
	body.Write(ev.Payload)
	body.WriteByte('}')
	return body.Bytes(), nil
}
