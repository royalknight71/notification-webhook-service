package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// Publisher reports success only after RabbitMQ confirms a routable persistent message.
type Publisher interface {
	Publish(ctx context.Context, eventID string, event []byte) error
}

type amqpPublisher struct {
	ch         *amqp091.Channel
	exchange   string
	routingKey string
	confirms   <-chan amqp091.Confirmation
	returns    <-chan amqp091.Return
	mu         sync.Mutex
	failed     error
}

func NewPublisher(conn *Connector) (Publisher, error) {
	if conn == nil || conn.Channel == nil {
		return nil, fmt.Errorf("publisher channel not initialized")
	}
	if err := conn.Channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return &amqpPublisher{
		ch: conn.Channel, exchange: conn.Exchange, routingKey: conn.RoutingKey,
		confirms: conn.Channel.NotifyPublish(make(chan amqp091.Confirmation, 1)),
		returns:  conn.Channel.NotifyReturn(make(chan amqp091.Return, 1)),
	}, nil
}

func (p *amqpPublisher) Publish(ctx context.Context, eventID string, event []byte) error {
	confirmCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch == nil {
		return fmt.Errorf("publisher channel not initialized")
	}
	if p.failed != nil {
		return fmt.Errorf("publisher channel is unusable after uncertain outcome: %w", p.failed)
	}
	err := p.ch.PublishWithContext(confirmCtx, p.exchange, p.routingKey, true, false, amqp091.Publishing{
		ContentType: "application/json", MessageId: eventID, Body: event, DeliveryMode: amqp091.Persistent,
	})
	if err != nil {
		p.failed = err
		return fmt.Errorf("publish message outcome is uncertain: %w", err)
	}
	select {
	case confirmation, ok := <-p.confirms:
		if !ok {
			p.failed = fmt.Errorf("publisher confirmation channel closed")
			return p.failed
		}
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ negatively confirmed event %s", eventID)
		}
		// RabbitMQ sends Basic.Return before the corresponding publisher confirm.
		select {
		case returned, ok := <-p.returns:
			if ok {
				return fmt.Errorf("event %s was unroutable: %s", eventID, returned.ReplyText)
			}
		default:
		}
		return nil
	case <-confirmCtx.Done():
		p.failed = fmt.Errorf("await publisher confirmation for %s: %w", eventID, confirmCtx.Err())
		return p.failed
	}
}
