package handler

import (
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/gin-gonic/gin"
)

func TestHealthEndpoint(t *testing.T) {
    // Set Gin to test mode to avoid unnecessary output.
    gin.SetMode(gin.TestMode)

    // Create a router with the health endpoint.
    router := gin.New()
    router.GET("/health", Health)

    // Perform a GET request.
    req, err := http.NewRequest(http.MethodGet, "/health", nil)
    if err != nil {
        t.Fatalf("failed to create request: %v", err)
    }
    w := httptest.NewRecorder()
    router.ServeHTTP(w, req)

    // Verify status code.
    if w.Code != http.StatusOK {
        t.Fatalf("expected status %d, got %d", http.StatusOK, w.Code)
    }

    // Verify JSON body.
    expectedBody := "{\"status\":\"ok\"}"
    if w.Body.String() != expectedBody {
        t.Fatalf("expected body %s, got %s", expectedBody, w.Body.String())
    }
}
