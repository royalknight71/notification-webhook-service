package handler

import (
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"

    "github.com/gin-gonic/gin"
)

// mockPublisher implements the queue.Publisher interface for testing.
type mockPublisher struct {
    published bool
    data      []byte
    err       error
}

func (m *mockPublisher) Publish(event []byte) error {
    m.published = true
    m.data = event
    return m.err
}

func (m *mockPublisher) Close() error { return nil }

func TestEventHandler_Success(t *testing.T) {
    gin.SetMode(gin.TestMode)
    router := gin.New()
    mp := &mockPublisher{}
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
    if !mp.published {
        t.Fatalf("publisher.Publish was not called")
    }
    var payload map[string]any
    if err := json.Unmarshal(mp.data, &payload); err != nil {
        t.Fatalf("published data not valid JSON: %v", err)
    }
    if payload["id"] != resp.EventID {
        t.Fatalf("published id %v does not match response id %s", payload["id"], resp.EventID)
    }
}

func TestEventHandler_InvalidJSON(t *testing.T) {
    gin.SetMode(gin.TestMode)
    router := gin.New()
    router.POST("/event", Event(&mockPublisher{}))

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
        t.Fatalf("expected status %d when publisher is nil, got %d", http.StatusServiceUnavailable, w.Code)
    }
}
