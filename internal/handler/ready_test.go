package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// mockDB implements the DB interface for testing.
type mockDB struct {
	err  error
	ping func(context.Context) error
}

func (m mockDB) Ping(ctx context.Context) error {
	if m.ping != nil {
		return m.ping(ctx)
	}
	return m.err
}

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

func TestReadyEndpoint_ConsumerStopped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/ready", Ready(mockDB{err: nil}, func() bool { return false }))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable || w.Body.String() != `{"status":"unavailable"}` {
		t.Fatalf("expected unavailable readiness, got %d %s", w.Code, w.Body.String())
	}
}

func TestReadyEndpointBoundsDatabasePing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := mockDB{ping: func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("readiness ping context has no deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	router := gin.New()
	router.GET("/ready", Ready(db))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected bounded ping to report unavailable, got %d", w.Code)
	}
}

func TestReadyEndpointLimitsConcurrentDatabasePings(t *testing.T) {
	gin.SetMode(gin.TestMode)
	started := make(chan struct{})
	var calls atomic.Int32
	db := mockDB{ping: func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	router := gin.New()
	router.GET("/ready", Ready(db))
	firstRequest, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil).WithContext(firstRequest))
		firstDone <- w.Code
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first readiness ping did not start")
	}
	second := httptest.NewRecorder()
	router.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("concurrent probe should fail fast, got %d", second.Code)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent readiness probes used %d database pings, want 1", got)
	}
	cancelFirst()
	select {
	case code := <-firstDone:
		if code != http.StatusServiceUnavailable {
			t.Fatalf("canceled first ping status=%d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("first readiness request did not finish after cancellation")
	}
}
