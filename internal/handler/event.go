package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-webhook-service/internal/delivery"
	"notification-webhook-service/internal/observability"
)

type EventRequest struct {
	TargetURL string          `json:"target_url,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

type EventStore interface {
	Accept(context.Context, delivery.Event) (bool, error)
}

// Event accepts an event after it and its outbox record have been committed.
func Event(store EventStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req EventRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON payload"})
			return
		}
		// Generate event ID.
		id := uuid.New().String()
		if store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event storage unavailable"})
			return
		}
		payload := req.Payload
		if len(payload) == 0 {
			payload = json.RawMessage("null")
		}
		inserted, err := store.Accept(c.Request.Context(), delivery.Event{ID: id, TargetURL: req.TargetURL, Payload: json.RawMessage(payload)})
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "event persistence failed", "event_id", id, "error", observability.ErrorSummary(err))
			if errors.Is(err, delivery.ErrIDConflict) {
				c.JSON(http.StatusConflict, gin.H{"error": "event ID conflict"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist event"})
			return
		}
		if !inserted {
			slog.InfoContext(c.Request.Context(), "duplicate event accepted as existing", "event_id", id)
			c.JSON(http.StatusConflict, gin.H{"error": "event already exists"})
			return
		}
		slog.InfoContext(c.Request.Context(), "event accepted", "event_id", id)
		c.JSON(http.StatusAccepted, gin.H{"event_id": id})
	}
}
