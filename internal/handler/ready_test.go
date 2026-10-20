package handler

import (
    "context"
    "errors"
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/gin-gonic/gin"
)

// mockDB implements the DB interface for testing.
type mockDB struct {
    err error
}

func (m mockDB) Ping(ctx context.Context) error { return m.err }

func TestReadyEndpoint_Success(t *testing.T) {
    gin.SetMode(gin.TestMode)
    router := gin.New()
    router.GET("/ready", Ready(mockDB{err: nil}))

    req, _ := http.NewRequest(http.MethodGet, "/ready", nil)
    w := httptest.NewRecorder()
    router.ServeHTTP(w, req)

    if w.Code != http.StatusOK {
        t.Fatalf("expected status %d, got %d", http.StatusOK, w.Code)
    }
    expected := "{\"status\":\"ready\"}"
    if w.Body.String() != expected {
        t.Fatalf("expected body %s, got %s", expected, w.Body.String())
    }
}

func TestReadyEndpoint_Failure(t *testing.T) {
    gin.SetMode(gin.TestMode)
    router := gin.New()
    router.GET("/ready", Ready(mockDB{err: errors.New("ping failed")}))

    req, _ := http.NewRequest(http.MethodGet, "/ready", nil)
    w := httptest.NewRecorder()
    router.ServeHTTP(w, req)

    if w.Code != http.StatusServiceUnavailable {
        t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
    }
    expected := "{\"status\":\"unavailable\"}"
    if w.Body.String() != expected {
        t.Fatalf("expected body %s, got %s", expected, w.Body.String())
    }
}
