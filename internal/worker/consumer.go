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
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"notification-webhook-service/internal/delivery"
	"notification-webhook-service/internal/observability"
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

// ConsumerHandle lets shutdown stop intake while allowing the current delivery
// to finish before the service context is canceled.
type ConsumerHandle struct {
	Done     <-chan struct{}
	Stopping <-chan struct{}
	stop     chan struct{}
	once     sync.Once
}

func (c *ConsumerHandle) StopAccepting() {
	c.once.Do(func() { close(c.stop) })
}

// StartConsumer starts a single-message-at-a-time worker. StopAccepting stops
// intake after the current delivery finishes; the service context remains the
// hard-stop path.
func StartConsumer(ctx context.Context, conn *queue.Connector, webhookSecret string, store DeliveryStore, deadLetters queue.DeadLetterPublisher, metrics ...*observability.Metrics) (*ConsumerHandle, error) {
	var metric *observability.Metrics
	if len(metrics) > 0 {
		metric = metrics[0]
	}
	// Processing is serial; a prefetch of one keeps shutdown from accumulating
	// unbounded unacknowledged deliveries in the client.
	if err := conn.Channel.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("configure consumer prefetch: %w", err)
	}
	msgs, err := conn.Channel.Consume(conn.Queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start consumer: %w", err)
	}
	done := make(chan struct{})
	stop := make(chan struct{})
	handle := &ConsumerHandle{Done: done, Stopping: stop, stop: stop}
	client := &http.Client{Timeout: 5 * time.Second}
	go func() {
		defer close(done)
		metric.SetConsumerRunning(true)
		defer metric.SetConsumerRunning(false)
		retryAttempt := 0
		for {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-handle.stop:
				return
			default:
			}
			select {
			case <-ctx.Done():
				return
			case <-handle.stop:
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}
				select {
				case <-handle.stop:
					return
				default:
				}
				acked, err := processDelivery(ctx, &d, client, webhookSecret, store, deadLetters, metric)
				if err != nil {
					slog.ErrorContext(ctx, "delivery processing failed", "event_id", d.MessageId, "error", observability.ErrorSummary(err))
					if ctx.Err() != nil {
						return
					}
					retryAttempt++
					if !waitRetry(ctx, retryBackoff(retryAttempt)) {
						return
					}
					if nackErr := d.Nack(false, true); nackErr != nil {
						slog.ErrorContext(ctx, "delivery nack failed", "event_id", d.MessageId, "error", observability.ErrorSummary(nackErr))
					}
				} else if acked {
					retryAttempt = 0
				}
			}
		}
	}()
	return handle, nil
}

func processDelivery(ctx context.Context, d *amqp091.Delivery, client *http.Client, webhookSecret string, store DeliveryStore, deadLetters queue.DeadLetterPublisher, metrics ...*observability.Metrics) (bool, error) {
	err := processEvent(ctx, d, client, webhookSecret, store, metrics...)
	if err == nil {
		if ackErr := d.Ack(false); ackErr != nil {
			slog.ErrorContext(ctx, "delivery ack failed", "event_id", d.MessageId, "error", observability.ErrorSummary(ackErr))
			return false, nil
		}
		return true, nil
	}
	if errors.Is(err, errTerminal) {
		err = routeTerminalMessage(ctx, deadLetters, d.MessageId, d.Body, func() error { return d.Ack(false) })
		if err != nil {
			slog.ErrorContext(ctx, "dead-letter routing failed", "event_id", d.MessageId, "error", observability.ErrorSummary(err))
		} else {
			slog.InfoContext(ctx, "dead-letter routing succeeded", "event_id", d.MessageId)
		}
		return err == nil, err
	}
	return false, err
}

var errTerminal = errors.New("terminal delivery failure")
var errDeliveryBusy = errors.New("delivery is claimed by another worker")

func waitBusyRetry(ctx context.Context, delay time.Duration) bool {
	return waitRetry(ctx, delay)
}

func waitRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func retryBackoff(attempt int) time.Duration {
	const (
		baseDelay = 250 * time.Millisecond
		maxDelay  = 5 * time.Second
	)
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	delay := baseDelay * time.Duration(1<<uint(shift))
	if delay > maxDelay {
		delay = maxDelay
	}
	jitter := time.Duration(rand.Int63n(int64(delay/2) + 1))
	if delay+jitter > maxDelay {
		return maxDelay
	}
	return delay + jitter
}

func routeTerminalMessage(ctx context.Context, publisher queue.DeadLetterPublisher, messageID string, body []byte, ack func() error) error {
	if publisher == nil {
		return fmt.Errorf("dead-letter publisher is unavailable")
	}
	if err := publisher.PublishDeadLetter(ctx, messageID, body); err != nil {
		return err
	}
	return ack()
}

func releaseDelivery(store DeliveryStore, eventID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.ReleaseDelivery(ctx, eventID); err != nil {
		slog.Error("delivery claim release failed", "event_id", eventID, "error", observability.ErrorSummary(err))
	}
}

func processEvent(ctx context.Context, d *amqp091.Delivery, client *http.Client, webhookSecret string, store DeliveryStore, metrics ...*observability.Metrics) error {
	var metric *observability.Metrics
	if len(metrics) > 0 {
		metric = metrics[0]
	}
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
	if claim == delivery.ClaimFailed {
		return fmt.Errorf("event %s is already failed: %w", ev.ID, errTerminal)
	}
	if claim == delivery.ClaimNotFound {
		return fmt.Errorf("event %s has no delivery record: %w", ev.ID, errTerminal)
	}
	if claim == delivery.ClaimBusy {
		return errDeliveryBusy
	}
	const maxAttempts = 5
	for {
		if err := ctx.Err(); err != nil {
			releaseDelivery(store, ev.ID)
			return err
		}
		if attempt >= maxAttempts {
			if err := store.RecordAttemptError(ctx, ev.ID, "retry limit reached after process recovery", true); err != nil {
				releaseDelivery(store, ev.ID)
				return fmt.Errorf("persist exhausted state: %w", err)
			}
			metric.IncDeliveryOutcomeFailed()
			metric.IncDeliveryRetryExhausted()
			return fmt.Errorf("event %s exhausted %d attempts: %w", ev.ID, maxAttempts, errTerminal)
		}
		attempt, err = store.RecordAttempt(ctx, ev.ID)
		if err != nil {
			releaseDelivery(store, ev.ID)
			return fmt.Errorf("persist attempt: %w", err)
		}
		metric.IncDeliveryAttempt()
		if attempt > 1 {
			metric.IncDeliveryRetry()
		}
		slog.InfoContext(ctx, "webhook delivery attempt started", "event_id", ev.ID, "attempt", attempt)
		attemptStarted := time.Now()
		deliveryErr := deliverEvent(ctx, client, ev, webhookSecret)
		metric.ObserveDeliveryAttempt(time.Since(attemptStarted).Seconds())
		if deliveryErr == nil {
			if err := store.MarkDelivered(ctx, ev.ID); err != nil {
				releaseDelivery(store, ev.ID)
				return fmt.Errorf("persist delivered state: %w", err)
			}
			metric.IncDeliveryOutcomeSucceeded()
			slog.InfoContext(ctx, "webhook delivery succeeded", "event_id", ev.ID, "attempt", attempt)
			return nil
		}
		if ctx.Err() != nil {
			releaseDelivery(store, ev.ID)
			return ctx.Err()
		}
		final := attempt >= maxAttempts
		if err := store.RecordAttemptError(ctx, ev.ID, deliveryErr.Error(), final); err != nil {
			releaseDelivery(store, ev.ID)
			return fmt.Errorf("persist attempt failure: %w", err)
		}
		if final {
			metric.IncDeliveryOutcomeFailed()
			metric.IncDeliveryRetryExhausted()
			slog.ErrorContext(ctx, "webhook delivery permanently failed", "event_id", ev.ID, "attempt", attempt, "error", observability.ErrorSummary(deliveryErr))
			return fmt.Errorf("event %s failed after %d attempts: %w", ev.ID, attempt, errTerminal)
		}
		backoff := time.Duration(100*int64(1<<uint(attempt-1))) * time.Millisecond
		jitter := time.Duration(randInt63n(100)) * time.Millisecond
		slog.WarnContext(ctx, "webhook delivery will retry", "event_id", ev.ID, "attempt", attempt, "retry_delay", backoff+jitter, "error", observability.ErrorSummary(deliveryErr))
		select {
		case <-time.After(backoff + jitter):
		case <-ctx.Done():
			releaseDelivery(store, ev.ID)
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
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return fmt.Errorf("non-2xx response %d", resp.StatusCode)
}

func randInt63n(n int64) int64 { return rand.Int63n(n) }
