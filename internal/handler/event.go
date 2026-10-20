package handler

import (
    "net/http"
    "github.com/gin-gonic/gin"
    "github.com/google/uuid"
    "notification-webhook-service/internal/queue"
    "encoding/json"
)

type EventRequest struct {
    TargetURL string          `json:"target_url,omitempty"`
    Payload   json.RawMessage `json:"payload"`
}

// Event returns a Gin handler that accepts a webhook event, assigns a UUID, and publishes it.
// If the publisher is nil (RabbitMQ unavailable), it returns 503 Service Unavailable.
func Event(publisher queue.Publisher) gin.HandlerFunc {
    return func(c *gin.Context) {
        var req EventRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON payload"})
            return
        }
        // Generate event ID.
        id := uuid.New().String()
        // Build envelope that includes ID, target URL and payload.
        envelope := map[string]any{
            "id":        id,
            "target_url": req.TargetURL,
            "payload":   req.Payload,
        }
        data, err := json.Marshal(envelope)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to serialize event"})
            return
        }
        if publisher == nil {
            c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event publishing unavailable"})
            return
        }
        if err := publisher.Publish(data); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish event"})
            return
        }
        c.JSON(http.StatusAccepted, gin.H{"event_id": id})
    }
}
