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
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"notification-webhook-service/internal/delivery"
)

type testStore struct {
	attempts           int
	state              string
	lastError          string
	failStatus         bool
	claimErr           error
	releaseHasDeadline bool
}

func (s *testStore) ClaimDelivery(_ context.Context, _ string, _ time.Duration) (delivery.ClaimResult, int, error) {
	if s.claimErr != nil {
		return 0, 0, s.claimErr
	}
	if s.state == "failed" {
		return delivery.ClaimFailed, s.attempts, nil
	}
	if s.state == "missing" {
		return delivery.ClaimNotFound, 0, nil
	}
	if s.state == "delivered" {
		return delivery.ClaimTerminal, s.attempts, nil
	}
	if s.state == "processing" {
		return delivery.ClaimBusy, s.attempts, nil
	}
	s.state = "processing"
	return delivery.Claimed, s.attempts, nil
}
func (s *testStore) RecordAttempt(context.Context, string) (int, error) {
	s.attempts++
	return s.attempts, nil
}
func (s *testStore) RecordAttemptError(_ context.Context, _ string, msg string, final bool) error {
	if s.failStatus {
		return fmt.Errorf("database unavailable")
	}
	s.lastError = msg
	if final {
		s.state = "failed"
	}
	return nil
}
func (s *testStore) MarkDelivered(context.Context, string) error {
	if s.failStatus {
		return fmt.Errorf("database unavailable")
	}
	s.state = "delivered"
	return nil
}
func (s *testStore) ReleaseDelivery(ctx context.Context, _ string) error {
	_, s.releaseHasDeadline = ctx.Deadline()
	s.state = "pending"
	return nil
}

type testDeadLetterPublisher struct {
	err   error
	id    string
	body  []byte
	calls int
}

func (p *testDeadLetterPublisher) PublishDeadLetter(_ context.Context, id string, body []byte) error {
	p.calls++
	p.id = id
	p.body = append([]byte(nil), body...)
	return p.err
}

type testAcknowledger struct {
	acks  int
	nacks int
}

func (a *testAcknowledger) Ack(uint64, bool) error {
	a.acks++
	return nil
}

func (a *testAcknowledger) Nack(uint64, bool, bool) error {
	a.nacks++
	return nil
}

func (*testAcknowledger) Reject(uint64, bool) error { return nil }

// mockTransport implements http.RoundTripper and allows configuring responses.
type mockTransport struct {
	status  int
	body    string
	err     error
	mu      sync.Mutex
	calls   int
	handler func(*http.Request)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	if m.handler != nil {
		m.handler(req)
	}
	if m.err != nil {
		return nil, m.err
	}
	resp := &http.Response{
		StatusCode: m.status,
		Body:       io.NopCloser(strings.NewReader(m.body)),
		Header:     make(http.Header),
	}
	return resp, nil
}

func TestProcessEvent_Success(t *testing.T) {
	ctx := context.Background()
	mt := &mockTransport{status: http.StatusOK, body: "ok"}
	client := &http.Client{Transport: mt}
	payload := json.RawMessage(`{"msg":"hello"}`)
	ev := EventMessage{ID: "123", TargetURL: "http://example.com", Payload: payload}
	bodyBytes, _ := json.Marshal(ev)
	d := amqp091.Delivery{Body: bodyBytes}
	store := &testStore{}
	if err := processEvent(ctx, &d, client, "", store); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	mt.mu.Lock()
	calls := mt.calls
	mt.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", calls)
	}
	if store.state != "delivered" || store.attempts != 1 {
		t.Fatalf("unexpected persisted state=%s attempts=%d", store.state, store.attempts)
	}
}

func TestProcessEvent_RetryAndFailure(t *testing.T) {
	ctx := context.Background()
	mt := &mockTransport{status: http.StatusInternalServerError, body: "fail"}
	client := &http.Client{Transport: mt}
	payload := json.RawMessage(`{"msg":"hi"}`)
	ev := EventMessage{ID: "abc", TargetURL: "http://example.com", Payload: payload}
	bodyBytes, _ := json.Marshal(ev)
	d := amqp091.Delivery{Body: bodyBytes}
	store := &testStore{}
	err := processEvent(ctx, &d, client, "", store)
	if err == nil {
		t.Fatalf("expected error after retries, got nil")
	}
	mt.mu.Lock()
	calls := mt.calls
	mt.mu.Unlock()
	if calls != 5 {
		t.Fatalf("expected 5 HTTP calls, got %d", calls)
	}
	if store.state != "failed" || store.attempts != 5 || store.lastError == "" {
		t.Fatalf("failure state not persisted: %#v", store)
	}
}

func TestProcessEvent_HMACSignature(t *testing.T) {
	secret := "mysecret"
	var receivedSig string
	var receivedBody []byte
	mt := &mockTransport{status: http.StatusOK, body: "ok", handler: func(r *http.Request) {
		receivedSig = r.Header.Get("X-Signature")
		receivedBody, _ = io.ReadAll(r.Body)
	}}
	client := &http.Client{Transport: mt}
	payload := json.RawMessage("{\n  \"z\": 1.00,\n  \"a\": 2e+03\n}")
	ev := EventMessage{ID: "sig1", TargetURL: "http://example.com", Payload: payload}
	bodyBytes := rawEventMessage(t, ev)
	d := amqp091.Delivery{Body: bodyBytes}
	store := &testStore{}
	if err := processEvent(context.Background(), &d, client, secret, store); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if receivedSig != expected {
		t.Fatalf("signature mismatch: got %s, want %s", receivedSig, expected)
	}
	if !bytes.Equal(receivedBody, payload) {
		t.Fatalf("webhook payload bytes changed: got %q, want %q", receivedBody, payload)
	}
}

func TestRetryBackoffHasJitterAndIsBounded(t *testing.T) {
	for i := 0; i < 100; i++ {
		delay := retryBackoff(1)
		if delay < 250*time.Millisecond || delay > 375*time.Millisecond {
			t.Fatalf("first retry delay out of bounds: %s", delay)
		}
		if got := retryBackoff(100); got > 5*time.Second {
			t.Fatalf("capped retry delay exceeded maximum: %s", got)
		}
	}
	for i := 0; i < 100; i++ {
		jitter := randInt63n(100)
		if jitter < 0 || jitter >= 100 {
			t.Fatalf("jitter out of range: %d", jitter)
		}
	}
}

func TestWaitRetryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitRetry(ctx, time.Second) {
		t.Fatal("retry wait should stop on cancellation")
	}
}

func TestProcessEvent_ClaimFailureRemainsRetryable(t *testing.T) {
	store := &testStore{claimErr: errors.New("database unavailable")}
	mt := &mockTransport{status: http.StatusOK}
	ev := EventMessage{ID: "db-failure", TargetURL: "http://example.com", Payload: json.RawMessage(`{"ok":true}`)}
	body, _ := json.Marshal(ev)
	if err := processEvent(context.Background(), &amqp091.Delivery{Body: body}, &http.Client{Transport: mt}, "", store); err == nil {
		t.Fatal("expected claim failure")
	} else if errors.Is(err, errTerminal) {
		t.Fatalf("database failure was incorrectly classified terminal: %v", err)
	}
	if store.state == "failed" || mt.calls != 0 {
		t.Fatalf("database failure changed terminal state or made HTTP call: state=%s calls=%d", store.state, mt.calls)
	}
}

func TestRouteTerminalMessagePublishesBeforeAck(t *testing.T) {
	body := []byte("unparseable message")
	publisher := &testDeadLetterPublisher{}
	acked := false
	err := routeTerminalMessage(context.Background(), publisher, "message-1", body, func() error {
		acked = true
		return nil
	})
	if err != nil || !acked || publisher.id != "message-1" || !bytes.Equal(publisher.body, body) {
		t.Fatalf("terminal route err=%v acked=%v id=%q body=%q", err, acked, publisher.id, publisher.body)
	}

	publisher.err = errors.New("publisher unavailable")
	acked = false
	if err := routeTerminalMessage(context.Background(), publisher, "message-2", body, func() error {
		acked = true
		return nil
	}); err == nil || acked {
		t.Fatalf("failed dead-letter publish must not ACK: err=%v acked=%v", err, acked)
	}
}

func TestProcessDeliveryAcksAlreadyDeliveredDuplicate(t *testing.T) {
	d, acker := deliveryForTestMessage(t)
	store := &testStore{state: "delivered"}
	deadLetters := &testDeadLetterPublisher{}
	acked, err := processDelivery(context.Background(), d, &http.Client{}, "", store, deadLetters)
	if err != nil || !acked || acker.acks != 1 || deadLetters.calls != 0 {
		t.Fatalf("duplicate handling: acked=%v err=%v ACKs=%d dead-letter publishes=%d", acked, err, acker.acks, deadLetters.calls)
	}
}

func TestProcessDeliveryRoutesFailedDeliveryBeforeAck(t *testing.T) {
	d, acker := deliveryForTestMessage(t)
	store := &testStore{state: "failed"}
	deadLetters := &testDeadLetterPublisher{}
	acked, err := processDelivery(context.Background(), d, &http.Client{}, "", store, deadLetters)
	if err != nil || !acked || acker.acks != 1 || deadLetters.calls != 1 {
		t.Fatalf("failed delivery handling: acked=%v err=%v ACKs=%d dead-letter publishes=%d", acked, err, acker.acks, deadLetters.calls)
	}
}

func TestProcessDeliveryRetriesFailedDeadLetterPublishOnRedelivery(t *testing.T) {
	store := &testStore{state: "failed"}
	deadLetters := &testDeadLetterPublisher{err: errors.New("broker unavailable")}
	first, firstAcker := deliveryForTestMessage(t)
	acked, err := processDelivery(context.Background(), first, &http.Client{}, "", store, deadLetters)
	if err == nil || acked || firstAcker.acks != 0 {
		t.Fatalf("failed DLQ publish must remain unacked: acked=%v err=%v ACKs=%d", acked, err, firstAcker.acks)
	}

	deadLetters.err = nil
	second, secondAcker := deliveryForTestMessage(t)
	acked, err = processDelivery(context.Background(), second, &http.Client{}, "", store, deadLetters)
	if err != nil || !acked || secondAcker.acks != 1 || deadLetters.calls != 2 {
		t.Fatalf("redelivery handling: acked=%v err=%v ACKs=%d dead-letter publishes=%d", acked, err, secondAcker.acks, deadLetters.calls)
	}
}

func TestProcessDeliveryRoutesUnknownDeliveryBeforeAck(t *testing.T) {
	d, acker := deliveryForTestMessage(t)
	store := &testStore{state: "missing"}
	deadLetters := &testDeadLetterPublisher{}
	acked, err := processDelivery(context.Background(), d, &http.Client{}, "", store, deadLetters)
	if err != nil || !acked || acker.acks != 1 || deadLetters.calls != 1 {
		t.Fatalf("unknown delivery handling: acked=%v err=%v ACKs=%d dead-letter publishes=%d", acked, err, acker.acks, deadLetters.calls)
	}
}

func TestProcessDeliveryRetriesUnknownDeliveryAfterDeadLetterFailure(t *testing.T) {
	store := &testStore{state: "missing"}
	deadLetters := &testDeadLetterPublisher{err: errors.New("broker unavailable")}
	first, firstAcker := deliveryForTestMessage(t)
	acked, err := processDelivery(context.Background(), first, &http.Client{}, "", store, deadLetters)
	if err == nil || acked || firstAcker.acks != 0 {
		t.Fatalf("failed DLQ publish must leave unknown delivery unacked: acked=%v err=%v ACKs=%d", acked, err, firstAcker.acks)
	}

	deadLetters.err = nil
	second, secondAcker := deliveryForTestMessage(t)
	acked, err = processDelivery(context.Background(), second, &http.Client{}, "", store, deadLetters)
	if err != nil || !acked || secondAcker.acks != 1 || deadLetters.calls != 2 {
		t.Fatalf("unknown delivery redelivery: acked=%v err=%v ACKs=%d dead-letter publishes=%d", acked, err, secondAcker.acks, deadLetters.calls)
	}
}

func deliveryForTestMessage(t *testing.T) (*amqp091.Delivery, *testAcknowledger) {
	acker := &testAcknowledger{}
	body := rawEventMessage(t, EventMessage{ID: "failed-event", TargetURL: "https://example.test", Payload: json.RawMessage(`{"ok":true}`)})
	return &amqp091.Delivery{Acknowledger: acker, DeliveryTag: 1, MessageId: "failed-event", Body: body}, acker
}

func TestReleaseDeliveryUsesBoundedCleanupContext(t *testing.T) {
	store := &testStore{}
	releaseDelivery(store, "release")
	if !store.releaseHasDeadline {
		t.Fatal("release cleanup context has no deadline")
	}
}

func rawEventMessage(t *testing.T, ev EventMessage) []byte {
	t.Helper()
	id, err := json.Marshal(ev.ID)
	if err != nil {
		t.Fatal(err)
	}
	targetURL, err := json.Marshal(ev.TargetURL)
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(`{"id":`), id...)
	body = append(body, []byte(`,"target_url":`)...)
	body = append(body, targetURL...)
	body = append(body, []byte(`,"payload":`)...)
	body = append(body, ev.Payload...)
	return append(body, '}')
}

func TestProcessEvent_Idempotency(t *testing.T) {
	ctx := context.Background()
	mt := &mockTransport{status: http.StatusOK, body: "ok"}
	client := &http.Client{Transport: mt}
	payload := json.RawMessage(`{"x":1}`)
	ev := EventMessage{ID: "dup", TargetURL: "http://example.com", Payload: payload}
	bodyBytes, _ := json.Marshal(ev)
	d := amqp091.Delivery{Body: bodyBytes}
	store := &testStore{}
	if err := processEvent(ctx, &d, client, "", store); err != nil {
		t.Fatalf("first call error: %v", err)
	}
	mt.mu.Lock()
	callsBefore := mt.calls
	mt.mu.Unlock()
	if err := processEvent(ctx, &d, client, "", store); err != nil {
		t.Fatalf("second call error: %v", err)
	}
	mt.mu.Lock()
	callsAfter := mt.calls
	mt.mu.Unlock()
	if callsAfter != callsBefore {
		t.Fatalf("idempotent call made extra HTTP request: before %d after %d", callsBefore, callsAfter)
	}
}

func TestProcessEvent_ContextCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mt := &mockTransport{err: fmt.Errorf("network error")}
	client := &http.Client{Transport: mt}
	payload := json.RawMessage(`{"y":2}`)
	ev := EventMessage{ID: "canc", TargetURL: "http://example.com", Payload: payload}
	bodyBytes, _ := json.Marshal(ev)
	d := amqp091.Delivery{Body: bodyBytes}
	store := &testStore{}
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := processEvent(ctx, &d, client, "", store)
	if err == nil || err != context.Canceled {
		t.Fatalf("expected context cancellation error, got %v", err)
	}
	if store.state != "pending" {
		t.Fatalf("canceled delivery was not released: %s", store.state)
	}
}

func TestProcessEvent_StatusUpdateFailureRequeues(t *testing.T) {
	store := &testStore{failStatus: true}
	mt := &mockTransport{status: http.StatusOK}
	client := &http.Client{Transport: mt}
	ev := EventMessage{ID: "status", TargetURL: "http://example.com", Payload: json.RawMessage(`{"ok":true}`)}
	body, _ := json.Marshal(ev)
	d := amqp091.Delivery{Body: body}
	if err := processEvent(context.Background(), &d, client, "", store); err == nil {
		t.Fatal("expected status persistence failure")
	}
	if store.state != "pending" {
		t.Fatalf("delivery not released after status failure: %s", store.state)
	}
}

func TestProcessEvent_FailedEventRemainsDeadLetterable(t *testing.T) {
	store := &testStore{}
	bad := &mockTransport{status: http.StatusInternalServerError}
	client := &http.Client{Transport: bad}
	ev := EventMessage{ID: "retryable", TargetURL: "http://example.com", Payload: json.RawMessage(`{"x":1}`)}
	body, _ := json.Marshal(ev)
	d := amqp091.Delivery{Body: body}
	_ = processEvent(context.Background(), &d, client, "", store)
	if store.state != "failed" {
		t.Fatalf("expected failed terminal state, got %s", store.state)
	}
	before := bad.calls
	if err := processEvent(context.Background(), &d, client, "", store); !errors.Is(err, errTerminal) {
		t.Fatalf("failed duplicate should remain terminal for dead-letter routing, got %v", err)
	}
	if bad.calls != before {
		t.Fatalf("failed duplicate made another HTTP call: %d to %d", before, bad.calls)
	}
}

func TestProcessEvent_AttemptStatusFailureRequeues(t *testing.T) {
	store := &testStore{failStatus: true}
	mt := &mockTransport{status: http.StatusInternalServerError}
	client := &http.Client{Transport: mt}
	ev := EventMessage{ID: "attempt-status", TargetURL: "http://example.com", Payload: json.RawMessage(`{"x":1}`)}
	body, _ := json.Marshal(ev)
	d := amqp091.Delivery{Body: body}
	if err := processEvent(context.Background(), &d, client, "", store); err == nil {
		t.Fatal("expected attempt status persistence failure")
	}
	if mt.calls != 1 || store.state != "pending" {
		t.Fatalf("calls=%d state=%s", mt.calls, store.state)
	}
}

func TestWaitBusyRetryDelaysAndCancels(t *testing.T) {
	started := time.Now()
	if !waitBusyRetry(context.Background(), 40*time.Millisecond) {
		t.Fatal("busy retry delay unexpectedly canceled")
	}
	if time.Since(started) < 35*time.Millisecond {
		t.Fatal("busy retry returned before its delay")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitBusyRetry(ctx, time.Second) {
		t.Fatal("busy retry should stop promptly on cancellation")
	}
}

func TestConsumerStopAcceptingIsIdempotent(t *testing.T) {
	stopping := make(chan struct{})
	handle := &ConsumerHandle{Stopping: stopping, stop: stopping}
	handle.StopAccepting()
	handle.StopAccepting()
	select {
	case <-handle.Stopping:
	default:
		t.Fatal("consumer stop signal was not closed")
	}
}
