package worker

import (
    "bytes"
    "context"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "sync"
    "time"

    "github.com/rabbitmq/amqp091-go"
    "notification-webhook-service/internal/queue"
)

type EventMessage struct {
    ID        string          `json:"id"`
    TargetURL string          `json:"target_url,omitempty"`
    Payload   json.RawMessage `json:"payload"`
}

// StartConsumer starts a goroutine that consumes messages from the RabbitMQ queue
// and delivers them to the configured webhook endpoint. It runs until the
// provided context is cancelled.
func StartConsumer(ctx context.Context, conn *queue.Connector, webhookSecret string) error {
    ch := conn.Channel
    msgs, err := ch.Consume(
        conn.Queue,
        "",    // consumer tag (empty – auto generated)
        false, // autoAck
        false, // exclusive
        false, // noLocal
        false, // noWait
        nil,   // args
    )
    if err != nil {
        return fmt.Errorf("failed to start consumer: %w", err)
    }

    // in‑memory map of processed IDs for idempotency
    var processed sync.Map // map[string]struct{}

    // simple HTTP client with timeout
    httpClient := &http.Client{Timeout: 5 * time.Second}

    go func() {
        for {
            select {
            case <-ctx.Done():
                return
            case d, ok := <-msgs:
                if !ok {
                    return
                }
                // Process the delivery; handle ack/nack based on result.
                if err := processEvent(ctx, &d, httpClient, webhookSecret, &processed); err != nil {
                    log.Printf("event processing error: %v", err)
                    // NACK without requeue on failure (could be refined)
                    d.Nack(false, false)
                } else {
                    d.Ack(false)
                }
            }
        }
    }()
    return nil
}

// processEvent contains the core logic for handling a single delivery. It is
// extracted to make unit testing easier without needing a real AMQP channel.
func processEvent(ctx context.Context, d *amqp091.Delivery, client *http.Client, webhookSecret string, processed *sync.Map) error {
    var ev EventMessage
    if err := json.Unmarshal(d.Body, &ev); err != nil {
        return fmt.Errorf("invalid message format: %w", err)
    }
    // Idempotency check – if already processed, treat as success.
    if _, loaded := processed.LoadOrStore(ev.ID, struct{}{}); loaded {
        return nil
    }
    attempt := 0
    const maxAttempts = 5
    for {
        attempt++
        err := deliverEvent(ctx, client, ev, webhookSecret)
        if err == nil {
            return nil
        }
        if attempt >= maxAttempts {
            return fmt.Errorf("event %s failed after %d attempts: %w", ev.ID, attempt, err)
        }
        // exponential backoff with jitter
        backoff := time.Duration(100*int64(1<<uint(attempt-1))) * time.Millisecond
        jitter := time.Duration(randInt63n(100)) * time.Millisecond
        select {
        case <-time.After(backoff + jitter):
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

func deliverEvent(ctx context.Context, client *http.Client, ev EventMessage, secret string) error {
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, ev.TargetURL, io.NopCloser(bytes.NewReader(ev.Payload)))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")
    if secret != "" {
        mac := hmac.New(sha256.New, []byte(secret))
        mac.Write(ev.Payload)
        signature := hex.EncodeToString(mac.Sum(nil))
        req.Header.Set("X-Signature", "sha256="+signature)
    }
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode >= 200 && resp.StatusCode < 300 {
        return nil
    }
    body, _ := io.ReadAll(resp.Body)
    return fmt.Errorf("non‑2xx response %d: %s", resp.StatusCode, string(body))
}

// randInt63n is a tiny wrapper around math/rand to avoid importing the whole package globally.
func randInt63n(n int64) int64 {
    return time.Now().UnixNano() % n // simple deterministic fallback; replace with crypto/rand if needed
}
