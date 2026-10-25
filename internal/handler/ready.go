package handler

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"notification-webhook-service/internal/observability"
)

// DB is a minimal interface for checking database connectivity.
// It matches the Ping method of *pgxpool.Pool.
type DB interface {
	Ping(context.Context) error
}

const readinessPingTimeout = time.Second

// Ready returns a Gin handler that reports the readiness of the service.
// It attempts to ping the provided database. If the ping succeeds, it returns
// HTTP 200 with a simple JSON payload. If the ping fails, it returns HTTP 503.
func Ready(db DB, workers ...func() bool) gin.HandlerFunc {
	var pingInProgress sync.Mutex
	return func(c *gin.Context) {
		for _, workerReady := range workers {
			if !workerReady() {
				c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
				return
			}
		}
		// Keep health probes from occupying many pool connections at once.
		if !pingInProgress.TryLock() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		defer pingInProgress.Unlock()

		pingCtx, cancel := context.WithTimeout(c.Request.Context(), readinessPingTimeout)
		defer cancel()
		if err := db.Ping(pingCtx); err != nil {
			slog.WarnContext(c.Request.Context(), "readiness database ping failed", "error", observability.ErrorSummary(err))
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	}
}
