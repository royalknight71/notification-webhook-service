package worker

import (
    "context"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "strings"
    "sync"
    "testing"
    "time"

    "github.com/rabbitmq/amqp091-go"
)

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
    processed := &sync.Map{}
    if err := processEvent(ctx, &d, client, "", processed); err != nil {
        t.Fatalf("expected nil error, got %v", err)
    }
    mt.mu.Lock()
    calls := mt.calls
    mt.mu.Unlock()
    if calls != 1 {
        t.Fatalf("expected 1 HTTP call, got %d", calls)
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
    processed := &sync.Map{}
    err := processEvent(ctx, &d, client, "", processed)
    if err == nil {
        t.Fatalf("expected error after retries, got nil")
    }
    mt.mu.Lock()
    calls := mt.calls
    mt.mu.Unlock()
    if calls != 5 {
        t.Fatalf("expected 5 HTTP calls, got %d", calls)
    }
}

func TestProcessEvent_HMACSignature(t *testing.T) {
    secret := "mysecret"
    var receivedSig string
    mt := &mockTransport{status: http.StatusOK, body: "ok", handler: func(r *http.Request) {
        receivedSig = r.Header.Get("X-Signature")
    }}
    client := &http.Client{Transport: mt}
    payload := json.RawMessage(`{"data":123}`)
    ev := EventMessage{ID: "sig1", TargetURL: "http://example.com", Payload: payload}
    bodyBytes, _ := json.Marshal(ev)
    d := amqp091.Delivery{Body: bodyBytes}
    processed := &sync.Map{}
    if err := processEvent(context.Background(), &d, client, secret, processed); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write(payload)
    expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
    if receivedSig != expected {
        t.Fatalf("signature mismatch: got %s, want %s", receivedSig, expected)
    }
}

func TestProcessEvent_Idempotency(t *testing.T) {
    ctx := context.Background()
    mt := &mockTransport{status: http.StatusOK, body: "ok"}
    client := &http.Client{Transport: mt}
    payload := json.RawMessage(`{"x":1}`)
    ev := EventMessage{ID: "dup", TargetURL: "http://example.com", Payload: payload}
    bodyBytes, _ := json.Marshal(ev)
    d := amqp091.Delivery{Body: bodyBytes}
    processed := &sync.Map{}
    if err := processEvent(ctx, &d, client, "", processed); err != nil {
        t.Fatalf("first call error: %v", err)
    }
    mt.mu.Lock()
    callsBefore := mt.calls
    mt.mu.Unlock()
    if err := processEvent(ctx, &d, client, "", processed); err != nil {
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
    processed := &sync.Map{}
    go func() {
        time.Sleep(20 * time.Millisecond)
        cancel()
    }()
    err := processEvent(ctx, &d, client, "", processed)
    if err == nil || err != context.Canceled {
        t.Fatalf("expected context cancellation error, got %v", err)
    }
}
