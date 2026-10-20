package queue

import "testing"

func TestPublisher_NilChannel(t *testing.T) {
    conn := &Connector{Channel: nil, Exchange: "ex", RoutingKey: "key"}
    p := NewPublisher(conn)
    if err := p.Publish([]byte(`{"test":1}`)); err == nil {
        t.Fatalf("expected error when channel is nil, got nil")
    }
}
