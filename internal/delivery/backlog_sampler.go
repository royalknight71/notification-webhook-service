package delivery

import (
	"context"
	"log/slog"
	"time"

	"notification-webhook-service/internal/observability"
)

type outboxBacklogReader interface {
	PendingOutboxCount(context.Context) (int64, error)
}

// StartOutboxBacklogSampler periodically refreshes the persisted pending-row
// gauge. Each query is bounded by the store's database operation timeout.
func StartOutboxBacklogSampler(ctx context.Context, store outboxBacklogReader, metrics *observability.Metrics, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	if interval <= 0 {
		interval = 15 * time.Second
	}
	go func() {
		defer close(done)
		refresh := func() {
			count, err := store.PendingOutboxCount(ctx)
			if err != nil {
				if ctx.Err() == nil {
					slog.ErrorContext(ctx, "sample outbox backlog failed", "error", observability.ErrorSummary(err))
				}
				return
			}
			metrics.SetOutboxPending(count)
		}
		refresh()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
	return done
}
