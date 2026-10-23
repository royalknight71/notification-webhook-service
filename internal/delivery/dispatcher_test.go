package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type fakePublisher struct {
	err   error
	calls int
	id    string
	body  []byte
}

type unhealthyPublisher struct{ fakePublisher }

func (unhealthyPublisher) Healthy() bool { return false }

func (p *fakePublisher) Publish(_ context.Context, id string, body []byte) error {
	p.calls++
	p.id = id
	p.body = body
	return p.err
}

type fakeOutbox struct {
	items            []OutboxItem
	marked, released int
	markErr          error
	claimLimit       int
	claimLease       time.Duration
}

func (s *fakeOutbox) ClaimOutbox(_ context.Context, limit int, lease time.Duration) ([]OutboxItem, error) {
	s.claimLimit = limit
	s.claimLease = lease
	return s.items, nil
}
func (s *fakeOutbox) MarkPublished(context.Context, string, string) error {
	s.marked++
	return s.markErr
}
func (s *fakeOutbox) ReleaseOutbox(context.Context, string, string) error { s.released++; return nil }

func TestDispatchBatchRequiresPositivePublishResult(t *testing.T) {
	store := &fakeOutbox{items: []OutboxItem{{Event: Event{ID: "event-1", TargetURL: "https://example.test", Payload: []byte(`{"ok":true}`)}, ClaimToken: "claim"}}}
	publisher := &fakePublisher{err: errors.New("confirm outcome uncertain")}
	dispatchBatch(context.Background(), store, publisher)
	if publisher.calls != 1 || store.marked != 0 || store.released != 1 {
		t.Fatalf("calls=%d marked=%d released=%d", publisher.calls, store.marked, store.released)
	}
	if store.claimLimit != 1 || store.claimLease != 30*time.Second {
		t.Fatalf("dispatcher claim limit/lease = %d/%s, want 1/30s", store.claimLimit, store.claimLease)
	}
}

func TestDispatchBatchMarksOnlyConfirmedEventPublished(t *testing.T) {
	store := &fakeOutbox{items: []OutboxItem{{Event: Event{ID: "event-2", TargetURL: "https://example.test", Payload: []byte(`null`)}, ClaimToken: "claim-2"}}}
	publisher := &fakePublisher{}
	dispatchBatch(context.Background(), store, publisher)
	if publisher.calls != 1 || store.marked != 1 || store.released != 0 {
		t.Fatalf("calls=%d marked=%d released=%d", publisher.calls, store.marked, store.released)
	}
	if publisher.id != "event-2" {
		t.Fatalf("published ID %q", publisher.id)
	}
	if store.claimLimit != 1 || store.claimLease != 30*time.Second {
		t.Fatalf("dispatcher claim limit/lease = %d/%s, want 1/30s", store.claimLimit, store.claimLease)
	}
}

func TestMarshalEventPreservesRawPayloadBytes(t *testing.T) {
	payload := []byte("{\n  \"z\": 1.00,\n  \"a\": 2e+03\n}")
	body, err := marshalEvent(Event{ID: "event-raw", TargetURL: "https://example.test", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(envelope.Payload, payload) {
		t.Fatalf("payload bytes changed in outbox envelope: got %q, want %q", envelope.Payload, payload)
	}
}

func TestDispatcherStopsWhenPublisherIsUnhealthy(t *testing.T) {
	store := &fakeOutbox{items: []OutboxItem{{Event: Event{ID: "event-1", TargetURL: "https://example.test", Payload: []byte(`null`)}, ClaimToken: "claim"}}}
	publisher := &unhealthyPublisher{fakePublisher: fakePublisher{err: errors.New("publisher failed")}}
	done := StartDispatcher(context.Background(), store, publisher)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after publisher became unhealthy")
	}
}
