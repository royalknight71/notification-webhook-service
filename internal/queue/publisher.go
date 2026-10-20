package queue

import (
    "encoding/json"
    "fmt"
    "github.com/rabbitmq/amqp091-go"
)

type Publisher interface {
    Publish(event []byte) error
    Close() error
}

// amqpPublisher implements Publisher using an AMQP channel.
type amqpPublisher struct {
    ch        *amqp091.Channel
    exchange  string
    routingKey string
}

// NewPublisher creates a Publisher from an existing Connector.
func NewPublisher(conn *Connector) Publisher {
    return &amqpPublisher{ch: conn.Channel, exchange: conn.Exchange, routingKey: conn.RoutingKey}
}

func (p *amqpPublisher) Publish(event []byte) error {
    if p.ch == nil {
        return fmt.Errorf("publisher channel not initialized")
    }
    // Ensure the message is marked as persistent.
    err := p.ch.Publish(
        p.exchange,
        p.routingKey,
        false, // mandatory
        false, // immediate
        amqp091.Publishing{
            ContentType: "application/json",
            Body:        event,
            DeliveryMode: amqp091.Persistent,
        },
    )
    if err != nil {
        return fmt.Errorf("failed to publish message: %w", err)
    }
    return nil
}

func (p *amqpPublisher) Close() error {
    if p.ch != nil {
        return p.ch.Close()
    }
    return nil
}

// Helper to marshal an event struct to JSON.
func MarshalEvent(v any) ([]byte, error) {
    return json.Marshal(v)
}
