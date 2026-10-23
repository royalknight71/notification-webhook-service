package queue

import (
	"errors"
	"testing"
)

func TestPublisher_NilChannel(t *testing.T) {
	conn := &Connector{Channel: nil, Exchange: "ex", RoutingKey: "key"}
	if _, err := NewPublisher(conn); err == nil {
		t.Fatalf("expected error when channel is nil, got nil")
	}
}

func TestPublisherHealthReportsUncertainFailure(t *testing.T) {
	p := &amqpPublisher{failed: errors.New("uncertain publish")}
	if p.Healthy() {
		t.Fatal("publisher with an uncertain failure reported healthy")
	}
}
