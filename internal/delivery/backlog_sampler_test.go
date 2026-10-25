package delivery

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"notification-webhook-service/internal/observability"
)

type fixedOutboxBacklog struct {
	count int64
	calls atomic.Int32
}

func (s *fixedOutboxBacklog) PendingOutboxCount(context.Context) (int64, error) {
	s.calls.Add(1)
	return s.count, nil
}

func TestOutboxBacklogSamplerSetsGaugeAndStops(t *testing.T) {
	metrics, err := observability.NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	store := &fixedOutboxBacklog{count: 4}
	ctx, cancel := context.WithCancel(context.Background())
	done := StartOutboxBacklogSampler(ctx, store, metrics, 10*time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if strings.Contains(response.Body.String(), "notification_outbox_pending 4") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(response.Body.String(), "notification_outbox_pending 4") {
		t.Fatalf("backlog gauge was not refreshed: %s", response.Body.String())
	}
	if store.calls.Load() == 0 {
		t.Fatal("backlog query was not called")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("backlog sampler did not stop after cancellation")
	}
}
