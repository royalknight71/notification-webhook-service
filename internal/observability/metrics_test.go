package observability

import (
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestMetricsEndpointExposesApplicationMetrics(t *testing.T) {
	metrics, err := NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	// Independent registries can be constructed without process-global name collisions.
	if _, err := NewMetrics(); err != nil {
		t.Fatalf("construct second metrics registry: %v", err)
	}

	metrics.IncEventAccepted()
	metrics.IncEventRejected()
	metrics.IncDeliveryAttempt()
	metrics.IncDeliveryRetry()
	metrics.IncDeliveryRetryExhausted()
	metrics.IncDeliveryOutcomeSucceeded()
	metrics.IncDeliveryOutcomeFailed()
	metrics.ObserveDeliveryAttempt(0.25)
	metrics.SetOutboxPending(7)
	metrics.SetConsumerRunning(true)
	metrics.SetDispatcherRunning(false)

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 {
		t.Fatalf("metrics endpoint status=%d", response.Code)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"notification_events_accepted_total",
		"notification_events_rejected_total",
		"notification_delivery_attempts_total",
		"notification_delivery_retries_total",
		"notification_delivery_retry_exhausted_total",
		"notification_delivery_outcomes_total",
		"notification_delivery_attempt_duration_seconds_bucket",
		"notification_outbox_pending",
		"notification_consumer_running",
		"notification_outbox_dispatcher_running",
	} {
		if !strings.Contains(string(body), name) {
			t.Errorf("metrics response missing %s", name)
		}
	}
	if strings.Contains(string(body), "event_id") || strings.Contains(string(body), "target_url") {
		t.Fatalf("metrics response contains high-cardinality identifiers: %s", body)
	}

	for _, expected := range []string{
		"notification_events_accepted_total 1",
		"notification_events_rejected_total 1",
		`notification_delivery_outcomes_total{outcome="succeeded"} 1`,
		`notification_delivery_outcomes_total{outcome="failed"} 1`,
		"notification_delivery_attempt_duration_seconds_count 1",
		"notification_outbox_pending 7",
		"notification_consumer_running 1",
		"notification_outbox_dispatcher_running 0",
	} {
		if !strings.Contains(string(body), expected) {
			t.Errorf("metrics response missing %q", expected)
		}
	}
}

func TestMetricsCountersSupportConcurrentUpdates(t *testing.T) {
	metrics, err := NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	const workers, updates = 12, 1000
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < updates; j++ {
				metrics.IncEventAccepted()
			}
		}()
	}
	wait.Wait()
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	want := "notification_events_accepted_total " + strconv.Itoa(workers*updates)
	if !strings.Contains(response.Body.String(), want) {
		t.Fatalf("concurrent counter missing %q:\n%s", want, response.Body.String())
	}
}
