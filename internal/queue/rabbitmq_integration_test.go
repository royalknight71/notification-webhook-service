package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

// Requires TEST_RABBITMQ_URL pointing to a disposable RabbitMQ vhost.
func TestRabbitMQPublishConfirmReturnAndDeadLetterIntegration(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("set TEST_RABBITMQ_URL to run RabbitMQ integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	suffix := uuid.NewString()
	exchange, queueName, key := "it.exchange."+suffix, "it.queue."+suffix, "it.key."+suffix
	t.Setenv("RABBITMQ_URL", url)
	t.Setenv("RABBITMQ_EXCHANGE", exchange)
	t.Setenv("RABBITMQ_QUEUE", queueName)
	t.Setenv("RABBITMQ_ROUTING_KEY", key)
	conn, err := InitConnector(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	defer func() {
		cleanup, err := conn.Conn.Channel()
		if err != nil {
			t.Errorf("open RabbitMQ cleanup channel: %v", err)
			return
		}
		defer cleanup.Close()
		for _, queueName := range []string{conn.Queue, conn.DeadLetterQueue} {
			if _, err := cleanup.QueueDelete(queueName, false, false, false); err != nil {
				t.Errorf("delete RabbitMQ test queue %s: %v", queueName, err)
			}
		}
		for _, exchange := range []string{conn.Exchange, conn.DeadLetterExchange} {
			if err := cleanup.ExchangeDelete(exchange, false, false); err != nil {
				t.Errorf("delete RabbitMQ test exchange %s: %v", exchange, err)
			}
		}
	}()
	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"integration","payload": { "n": 1.00 }}`)
	if err := publisher.Publish(ctx, "integration", body); err != nil {
		t.Fatalf("confirmed publish: %v", err)
	}
	message, ok, err := conn.Channel.Get(conn.Queue, false)
	if err != nil || !ok {
		t.Fatalf("get published work message: ok=%v err=%v", ok, err)
	}
	if message.MessageId != "integration" || string(message.Body) != string(body) {
		t.Fatalf("work message changed: id=%q body=%q", message.MessageId, message.Body)
	}
	if err := message.Ack(false); err != nil {
		t.Fatal(err)
	}
	if err := publisher.(DeadLetterPublisher).PublishDeadLetter(ctx, "terminal", body); err != nil {
		t.Fatalf("confirmed dead-letter publish: %v", err)
	}
	message, ok, err = conn.Channel.Get(conn.DeadLetterQueue, false)
	if err != nil || !ok {
		t.Fatalf("get dead-letter message: ok=%v err=%v", ok, err)
	}
	if message.MessageId != "terminal" || string(message.Body) != string(body) {
		t.Fatalf("dead-letter message changed: id=%q body=%q", message.MessageId, message.Body)
	}
	if err := message.Ack(false); err != nil {
		t.Fatal(err)
	}

	// An unroutable mandatory publish must fail even when the broker positively
	// confirms receipt of the message.
	ch, err := conn.Conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	unroutableExchange := "it.unroutable." + suffix
	if err := ch.ExchangeDeclare(unroutableExchange, amqp091.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	unroutable := &amqpPublisher{
		ch: ch, exchange: unroutableExchange, routingKey: "missing",
		confirms: ch.NotifyPublish(make(chan amqp091.Confirmation, 1)),
		returns:  ch.NotifyReturn(make(chan amqp091.Return, 1)),
	}
	if err := unroutable.Publish(ctx, "unroutable", body); err == nil {
		t.Fatal("mandatory unroutable publish unexpectedly succeeded")
	}
	if unroutable.Healthy() {
		t.Fatal("publisher remained healthy after unroutable message")
	}
	if err := ch.ExchangeDelete(unroutableExchange, false, false); err != nil {
		t.Fatal(err)
	}
}
