package queue

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Connector struct {
	Conn                 *amqp091.Connection
	Channel              *amqp091.Channel
	Exchange             string
	Queue                string
	RoutingKey           string
	DeadLetterExchange   string
	DeadLetterQueue      string
	DeadLetterRoutingKey string
}

// InitConnector establishes a connection to RabbitMQ, declares the exchange and queue, and binds them.
// It reads configuration from the environment variables:
//
//	RABBITMQ_URL        – full AMQP URL (required)
//	RABBITMQ_EXCHANGE   – name of the exchange (default "webhook_exchange")
//	RABBITMQ_QUEUE      – name of the durable queue (default "webhook_queue")
//	RABBITMQ_ROUTING_KEY – routing key used for publishing (default "webhook_key")
func InitConnector(ctx context.Context) (*Connector, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return nil, fmt.Errorf("RABBITMQ_URL not set")
	}
	// Use sensible defaults if variables are missing.
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	if exchange == "" {
		exchange = "webhook_exchange"
	}
	queueName := os.Getenv("RABBITMQ_QUEUE")
	if queueName == "" {
		queueName = "webhook_queue"
	}
	routingKey := os.Getenv("RABBITMQ_ROUTING_KEY")
	if routingKey == "" {
		routingKey = "webhook_key"
	}

	// Establish a connection with a short timeout to fail fast if broker is unavailable.
	dialCfg := amqp091.Config{Dial: amqp091.DefaultDial(30 * time.Second)}
	conn, err := amqp091.DialConfig(url, dialCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	// Ensure the connection is closed if any later step fails.
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}
	// Declare a durable direct exchange.
	if err := ch.ExchangeDeclare(
		exchange,
		amqp091.ExchangeDirect,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}
	// Declare a durable queue.
	if _, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}
	// Bind the queue to the exchange with the routing key.
	if err := ch.QueueBind(
		queueName,
		routingKey,
		exchange,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to bind queue: %w", err)
	}
	deadLetterExchange := exchange + ".dead-letter"
	deadLetterQueue := queueName + ".dead-letter"
	deadLetterRoutingKey := routingKey + ".dead-letter"
	if err := ch.ExchangeDeclare(
		deadLetterExchange,
		amqp091.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare dead-letter exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(deadLetterQueue, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare dead-letter queue: %w", err)
	}
	if err := ch.QueueBind(deadLetterQueue, deadLetterRoutingKey, deadLetterExchange, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to bind dead-letter queue: %w", err)
	}
	return &Connector{
		Conn: conn, Channel: ch, Exchange: exchange, Queue: queueName, RoutingKey: routingKey,
		DeadLetterExchange: deadLetterExchange, DeadLetterQueue: deadLetterQueue, DeadLetterRoutingKey: deadLetterRoutingKey,
	}, nil
}

// Close safely shuts down the channel and connection. It is safe to call multiple times.
func (c *Connector) Close() error {
	var firstErr error
	if c.Channel != nil {
		if err := c.Channel.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.Conn != nil {
		if err := c.Conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
