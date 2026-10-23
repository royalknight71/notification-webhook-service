package queue

import "testing"

func TestPublisher_NilChannel(t *testing.T) {
	conn := &Connector{Channel: nil, Exchange: "ex", RoutingKey: "key"}
	if _, err := NewPublisher(conn); err == nil {
		t.Fatalf("expected error when channel is nil, got nil")
	}
}
