package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-webhook-service/internal/delivery"
	"notification-webhook-service/internal/handler"
	"notification-webhook-service/internal/observability"
	"notification-webhook-service/internal/queue"
	"notification-webhook-service/internal/worker"
)

type capturedWebhook struct {
	body      []byte
	signature string
}

// Requires TEST_DATABASE_URL and TEST_RABBITMQ_URL pointing to disposable services.
func TestEndToEndEventDeliveryRetryAndDeadLetter(t *testing.T) {
	databaseURL, rabbitURL := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_RABBITMQ_URL")
	if databaseURL == "" || rabbitURL == "" {
		t.Skip("set TEST_DATABASE_URL and TEST_RABBITMQ_URL to run end-to-end integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, store, cleanupDB := newTestStore(t, ctx, databaseURL)
	suffix := uuid.NewString()
	t.Setenv("RABBITMQ_URL", rabbitURL)
	t.Setenv("RABBITMQ_EXCHANGE", "e2e.exchange."+suffix)
	t.Setenv("RABBITMQ_QUEUE", "e2e.queue."+suffix)
	t.Setenv("RABBITMQ_ROUTING_KEY", "e2e.key."+suffix)
	connector, err := queue.InitConnector(ctx)
	if err != nil {
		cleanupDB()
		t.Fatal(err)
	}
	publisher, err := queue.NewPublisher(connector)
	if err != nil {
		_ = connector.Close()
		cleanupDB()
		t.Fatal(err)
	}
	metrics, err := observability.NewMetrics()
	if err != nil {
		_ = connector.Close()
		cleanupDB()
		t.Fatal(err)
	}
	serviceCtx, stopService := context.WithCancel(ctx)
	consumer, err := worker.StartConsumer(serviceCtx, connector, "integration-secret", store, publisher.(queue.DeadLetterPublisher), metrics)
	if err != nil {
		stopService()
		_ = connector.Close()
		cleanupDB()
		t.Fatal(err)
	}
	consumerDone := consumer.Done
	dispatcherDone := delivery.StartDispatcher(serviceCtx, store, publisher, metrics)
	backlogDone := delivery.StartOutboxBacklogSampler(serviceCtx, store, metrics, 10*time.Millisecond)
	t.Cleanup(func() {
		consumer.StopAccepting()
		stopService()
		select {
		case <-consumerDone:
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop")
		}
		select {
		case <-dispatcherDone:
		case <-time.After(5 * time.Second):
			t.Error("dispatcher did not stop")
		}
		select {
		case <-backlogDone:
		case <-time.After(5 * time.Second):
			t.Error("backlog sampler did not stop")
		}
		cleanup, err := connector.Conn.Channel()
		if err != nil {
			t.Errorf("open RabbitMQ cleanup channel: %v", err)
		} else {
			for _, name := range []string{connector.Queue, connector.DeadLetterQueue} {
				if _, err := cleanup.QueueDelete(name, false, false, false); err != nil {
					t.Errorf("delete RabbitMQ test queue %s: %v", name, err)
				}
			}
			for _, name := range []string{connector.Exchange, connector.DeadLetterExchange} {
				if err := cleanup.ExchangeDelete(name, false, false); err != nil {
					t.Errorf("delete RabbitMQ test exchange %s: %v", name, err)
				}
			}
			_ = cleanup.Close()
		}
		_ = connector.Close()
		cleanupDB()
	})

	gin.SetMode(gin.TestMode)
	goodCalls := make(chan capturedWebhook, 8)
	var failedCalls atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path == "/fail" {
			failedCalls.Add(1)
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		goodCalls <- capturedWebhook{body: body, signature: r.Header.Get("X-Signature")}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer webhook.Close()
	router := gin.New()
	router.GET("/metrics", gin.WrapH(metrics.Handler()))
	router.POST("/event", handler.Event(store, metrics))
	api := httptest.NewServer(router)
	defer api.Close()

	rawPayload := []byte("{\n  \"z\": 1.00, \"a\": 2e+03\n}")
	goodID := postEvent(t, api.URL, webhook.URL+"/success", rawPayload)
	waitState(t, ctx, store, goodID, "delivered")
	select {
	case got := <-goodCalls:
		if !bytes.Equal(got.body, rawPayload) {
			t.Fatalf("webhook payload bytes changed: got %q want %q", got.body, rawPayload)
		}
		mac := hmac.New(sha256.New, []byte("integration-secret"))
		_, _ = mac.Write(rawPayload)
		wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if got.signature != wantSignature {
			t.Fatalf("signature=%q want %q", got.signature, wantSignature)
		}
	case <-ctx.Done():
		t.Fatal("webhook was not called")
	}
	var published bool
	if err := pool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM delivery_outbox WHERE event_id=$1`, goodID).Scan(&published); err != nil || !published {
		t.Fatalf("outbox published=%v err=%v", published, err)
	}

	duplicate, _ := json.Marshal(worker.EventMessage{ID: goodID, TargetURL: webhook.URL + "/success", Payload: rawPayload})
	if err := publisher.Publish(ctx, goodID, duplicate); err != nil {
		t.Fatalf("publish duplicate event: %v", err)
	}
	failedID := postEvent(t, api.URL, webhook.URL+"/fail", []byte(`{"retry":true}`))
	waitState(t, ctx, store, failedID, "failed")
	if calls := failedCalls.Load(); calls != 5 {
		t.Fatalf("failed webhook calls=%d, want 5 attempts", calls)
	}
	waitDeadLetter(t, ctx, connector, failedID)
	select {
	case extra := <-goodCalls:
		t.Fatalf("unexpected duplicate webhook call with body %q", extra.body)
	default:
	}

	unknownID := "unknown-" + uuid.NewString()
	unknown, _ := json.Marshal(worker.EventMessage{ID: unknownID, TargetURL: webhook.URL + "/success", Payload: json.RawMessage(`null`)})
	if err := publisher.Publish(ctx, unknownID, unknown); err != nil {
		t.Fatalf("publish unknown delivery: %v", err)
	}
	waitDeadLetter(t, ctx, connector, unknownID)
	if state, err := store.GetDeliveryState(ctx, unknownID); err == nil || state != "" {
		t.Fatalf("unknown delivery unexpectedly persisted: state=%q err=%v", state, err)
	}

	metricsResponse, err := http.Get(api.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metricsBody, readErr := io.ReadAll(metricsResponse.Body)
	_ = metricsResponse.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if metricsResponse.StatusCode != http.StatusOK {
		t.Fatalf("metrics endpoint status=%d body=%s", metricsResponse.StatusCode, metricsBody)
	}
	for _, expected := range []string{
		"notification_events_accepted_total 2",
		"notification_delivery_attempts_total 6",
		"notification_delivery_retries_total 4",
		"notification_delivery_retry_exhausted_total 1",
		`notification_delivery_outcomes_total{outcome="succeeded"} 1`,
		`notification_delivery_outcomes_total{outcome="failed"} 1`,
		"notification_delivery_attempt_duration_seconds_count 6",
		"notification_outbox_pending 0",
		"notification_consumer_running 1",
		"notification_outbox_dispatcher_running 1",
	} {
		if !strings.Contains(string(metricsBody), expected) {
			t.Errorf("metrics endpoint missing %q:\n%s", expected, metricsBody)
		}
	}
}

func newTestStore(t *testing.T, ctx context.Context, url string) (*pgxpool.Pool, *delivery.Store, func()) {
	t.Helper()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "e2e_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	store := delivery.NewStore(pool)
	if err := store.Migrate(ctx); err != nil {
		pool.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = admin.Exec(cleanupCtx, `DROP SCHEMA `+schema+` CASCADE`)
		cancel()
		admin.Close()
		t.Fatal(err)
	}
	cleanup := func() {
		pool.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = admin.Exec(cleanupCtx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	}
	return pool, store, cleanup
}

func postEvent(t *testing.T, apiURL, target string, payload []byte) string {
	t.Helper()
	targetJSON, _ := json.Marshal(target)
	requestBody := append([]byte(`{"target_url":`), targetJSON...)
	requestBody = append(requestBody, []byte(`,"payload":`)...)
	requestBody = append(requestBody, payload...)
	requestBody = append(requestBody, '}')
	req, err := http.NewRequest(http.MethodPost, apiURL+"/event", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /event status=%d body=%s", resp.StatusCode, body)
	}
	var result struct {
		ID string `json:"event_id"`
	}
	if err := json.Unmarshal(body, &result); err != nil || result.ID == "" {
		t.Fatalf("invalid event response %s: %v", body, err)
	}
	return result.ID
}

func waitState(t *testing.T, ctx context.Context, store *delivery.Store, id, want string) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := store.GetDeliveryState(ctx, id)
		if err == nil && state == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("delivery %s did not reach %s (last state=%q err=%v): %v", id, want, state, err, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitDeadLetter(t *testing.T, ctx context.Context, conn *queue.Connector, id string) {
	t.Helper()
	channel, err := conn.Conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		message, ok, err := channel.Get(conn.DeadLetterQueue, false)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if message.MessageId == id {
				if err := message.Ack(false); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err := message.Nack(false, true); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(fmt.Errorf("dead-letter message %s not observed: %w", id, ctx.Err()))
		case <-ticker.C:
		}
	}
}
