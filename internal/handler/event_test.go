package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"notification-webhook-service/internal/delivery"
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
