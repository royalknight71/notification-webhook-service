package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"notification-webhook-service/internal/delivery"
	"notification-webhook-service/internal/observability"
)

type mockEventStore struct {
	accepted bool
	event    delivery.Event
	err      error
}

func (m *mockEventStore) Accept(_ context.Context, event delivery.Event) (bool, error) {
	m.accepted = true
	m.event = event
	return true, m.err
}

func TestEventHandler_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	mp := &mockEventStore{}
	router.POST("/event", Event(mp))

	// valid JSON payload
	body := `{"target_url":"http://example.com","payload":{"foo":"bar"}}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}
	var resp struct {
		EventID string `json:"event_id"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.EventID == "" {
		t.Fatalf("expected non-empty event_id")
	}
	if !mp.accepted {
		t.Fatal("event was not persisted")
	}
	if mp.event.ID != resp.EventID {
		t.Fatalf("persisted id %q does not match response id %s", mp.event.ID, resp.EventID)
	}
	var payload map[string]any
	if err := json.Unmarshal(mp.event.Payload, &payload); err != nil {
		t.Fatalf("stored payload invalid: %v", err)
	}
	if mp.event.TargetURL != "http://example.com" {
		t.Fatalf("unexpected target URL %q", mp.event.TargetURL)
	}
	if payload["foo"] != "bar" {
		t.Fatalf("unexpected persisted payload %v", payload)
	}
}

func TestEventHandlerPreservesRawPayloadBytes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &mockEventStore{}
	router := gin.New()
	router.POST("/event", Event(store))
	payload := "{\n  \"z\": 1.00,\n  \"a\": 2e+03\n}"
	body := `{"target_url":"http://example.com","payload":` + payload + `}`
	req := httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}
	if string(store.event.Payload) != payload {
		t.Fatalf("payload bytes changed at request binding: got %q, want %q", store.event.Payload, payload)
	}
}

func TestEventHandler_InvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/event", Event(&mockEventStore{}))

	// malformed JSON
	body := `{"target_url":"http://example.com","payload":}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d for invalid JSON, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestEventHandler_NoPublisher(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/event", Event(nil))

	body := `{"target_url":"http://example.com","payload":{"foo":"bar"}}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d when store is nil, got %d", http.StatusServiceUnavailable, w.Code)
	}
}

func TestEventHandlerMetricsCountDurableAcceptanceAndRejections(t *testing.T) {
	gin.SetMode(gin.TestMode)
	metrics, err := observability.NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	store := &mockEventStore{}
	router := gin.New()
	router.POST("/event", Event(store, metrics))
	validBody := `{"target_url":"http://example.com","payload":{"ok":true}}`

	accepted := httptest.NewRecorder()
	router.ServeHTTP(accepted, httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(validBody)))
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("valid event status=%d", accepted.Code)
	}

	invalid := httptest.NewRecorder()
	router.ServeHTTP(invalid, httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(`{"payload":}`)))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid event status=%d", invalid.Code)
	}

	store.err = errors.New("injected persistence failure")
	persistenceFailure := httptest.NewRecorder()
	router.ServeHTTP(persistenceFailure, httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(validBody)))
	if persistenceFailure.Code != http.StatusInternalServerError {
		t.Fatalf("persistence failure status=%d", persistenceFailure.Code)
	}

	scrape := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text, err := io.ReadAll(scrape.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(text), "notification_events_accepted_total 1") {
		t.Fatalf("accepted metric did not reflect committed acceptance: %s", text)
	}
	if !strings.Contains(string(text), "notification_events_rejected_total 2") {
		t.Fatalf("rejected metric did not count invalid and failed requests: %s", text)
	}
}
