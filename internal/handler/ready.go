package handler

import (
    "context"
    "net/http"

    "github.com/gin-gonic/gin"
)

// DB is a minimal interface for checking database connectivity.
// It matches the Ping method of *pgxpool.Pool.
type DB interface {
    Ping(context.Context) error
}

// Ready returns a Gin handler that reports the readiness of the service.
// It attempts to ping the provided database. If the ping succeeds, it returns
// HTTP 200 with a simple JSON payload. If the ping fails, it returns HTTP 503.
func Ready(db DB) gin.HandlerFunc {
    return func(c *gin.Context) {
        // Use the request's context for cancellation and timeouts.
        if err := db.Ping(c.Request.Context()); err != nil {
            c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
            return
        }
        c.JSON(http.StatusOK, gin.H{"status": "ready"})
    }
}
