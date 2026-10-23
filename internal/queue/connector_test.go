package queue

import (
	"context"
	"testing"
)

func TestInitConnector_MissingURL(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "")
	// Ensure other vars are unset to avoid side effects
	t.Setenv("RABBITMQ_EXCHANGE", "")
	t.Setenv("RABBITMQ_QUEUE", "")
	t.Setenv("RABBITMQ_ROUTING_KEY", "")

	_, err := InitConnector(context.Background())
	if err == nil {
		t.Fatalf("expected error when RABBITMQ_URL is not set")
	}
	if err.Error() != "RABBITMQ_URL not set" {
		t.Fatalf("unexpected error message: %v", err)
	}
}
