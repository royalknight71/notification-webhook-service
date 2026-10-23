package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"notification-webhook-service/internal/delivery"
	"notification-webhook-service/internal/queue"
)

type EventMessage struct {
	ID        string          `json:"id"`
	TargetURL string          `json:"target_url,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

type DeliveryStore interface {
	ClaimDelivery(context.Context, string, time.Duration) (delivery.ClaimResult, int, error)
	RecordAttempt(context.Context, string) (int, error)
	RecordAttemptError(context.Context, string, string, bool) error
	MarkDelivered(context.Context, string) error
	ReleaseDelivery(context.Context, string) error
}

// StartConsumer starts a single-message-at-a-time worker and returns a channel
// closed after cancellation has been handled and the worker exits.
func StartConsumer(ctx context.Context, conn *queue.Connector, webhookSecret string, store DeliveryStore) (<-chan struct{}, error) {
	msgs, err := conn.Channel.Consume(conn.Queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start consumer: %w", err)
	}
	done := make(chan struct{})
	client := &http.Client{Timeout: 5 * time.Second}
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}
				if err := processEvent(ctx, &d, client, webhookSecret, store); err != nil {
					log.Printf("event processing error: %v", err)
					if errors.Is(err, errDeliveryBusy) && !waitBusyRetry(ctx, 250*time.Millisecond) {
						return
					}
					if nackErr := d.Nack(false, !errors.Is(err, errTerminal)); nackErr != nil {
						log.Printf("NACK event: %v", nackErr)
					}
				} else if ackErr := d.Ack(false); ackErr != nil {
					log.Printf("ACK event: %v", ackErr)
				}
			}
		}
	}()
	return done, nil
}

var errTerminal = errors.New("terminal delivery failure")
var errDeliveryBusy = errors.New("delivery is claimed by another worker")

func waitBusyRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func processEvent(ctx context.Context, d *amqp091.Delivery, client *http.Client, webhookSecret string, store DeliveryStore) error {
	var ev EventMessage
	if err := json.Unmarshal(d.Body, &ev); err != nil {
		return fmt.Errorf("invalid message format: %w", errTerminal)
	}
	if ev.ID == "" || ev.TargetURL == "" || len(ev.Payload) == 0 {
		return fmt.Errorf("incomplete event message: %w", errTerminal)
	}
	claim, attempt, err := store.ClaimDelivery(ctx, ev.ID, time.Minute)
	if err != nil {
		return fmt.Errorf("claim delivery: %w", err)
	}
	if claim == delivery.ClaimTerminal {
		return nil
	}
	if claim == delivery.ClaimBusy {
		return errDeliveryBusy
	}
	const maxAttempts = 5
	for {
		if err := ctx.Err(); err != nil {
			_ = store.ReleaseDelivery(context.Background(), ev.ID)
			return err
		}
		if attempt >= maxAttempts {
			if err := store.RecordAttemptError(ctx, ev.ID, "retry limit reached after process recovery", true); err != nil {
				_ = store.ReleaseDelivery(context.Background(), ev.ID)
				return fmt.Errorf("persist exhausted state: %w", err)
			}
			return fmt.Errorf("event %s exhausted %d attempts: %w", ev.ID, maxAttempts, errTerminal)
		}
		attempt, err = store.RecordAttempt(ctx, ev.ID)
		if err != nil {
			_ = store.ReleaseDelivery(context.Background(), ev.ID)
			return fmt.Errorf("persist attempt: %w", err)
		}
		deliveryErr := deliverEvent(ctx, client, ev, webhookSecret)
		if deliveryErr == nil {
			if err := store.MarkDelivered(ctx, ev.ID); err != nil {
				_ = store.ReleaseDelivery(context.Background(), ev.ID)
				return fmt.Errorf("persist delivered state: %w", err)
			}
			return nil
		}
		if ctx.Err() != nil {
			_ = store.ReleaseDelivery(context.Background(), ev.ID)
			return ctx.Err()
		}
		final := attempt >= maxAttempts
		if err := store.RecordAttemptError(ctx, ev.ID, deliveryErr.Error(), final); err != nil {
			_ = store.ReleaseDelivery(context.Background(), ev.ID)
			return fmt.Errorf("persist attempt failure: %w", err)
		}
		if final {
			return fmt.Errorf("event %s failed after %d attempts: %w", ev.ID, attempt, errTerminal)
		}
		backoff := time.Duration(100*int64(1<<uint(attempt-1))) * time.Millisecond
		jitter := time.Duration(randInt63n(100)) * time.Millisecond
		select {
		case <-time.After(backoff + jitter):
		case <-ctx.Done():
			_ = store.ReleaseDelivery(context.Background(), ev.ID)
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
		req.Header.Set("X-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
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
	return fmt.Errorf("non-2xx response %d: %s", resp.StatusCode, string(body))
}

func randInt63n(n int64) int64 { return time.Now().UnixNano() % n }
