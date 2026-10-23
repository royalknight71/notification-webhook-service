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

type DeadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, messageID string, body []byte) error
}

type HealthReporter interface {
	Healthy() bool
}

type amqpPublisher struct {
	ch                   *amqp091.Channel
	exchange             string
	routingKey           string
	deadLetterExchange   string
	deadLetterRoutingKey string
	confirms             <-chan amqp091.Confirmation
	returns              <-chan amqp091.Return
	mu                   sync.Mutex
	failed               error
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
		deadLetterExchange: conn.DeadLetterExchange, deadLetterRoutingKey: conn.DeadLetterRoutingKey,
		confirms: conn.Channel.NotifyPublish(make(chan amqp091.Confirmation, 1)),
		returns:  conn.Channel.NotifyReturn(make(chan amqp091.Return, 1)),
	}, nil
}

func (p *amqpPublisher) Publish(ctx context.Context, eventID string, event []byte) error {
	return p.publish(ctx, p.exchange, p.routingKey, eventID, event)
}

func (p *amqpPublisher) PublishDeadLetter(ctx context.Context, messageID string, body []byte) error {
	return p.publish(ctx, p.deadLetterExchange, p.deadLetterRoutingKey, messageID, body)
}

func (p *amqpPublisher) Healthy() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch != nil && !p.ch.IsClosed() && p.failed == nil
}

func (p *amqpPublisher) publish(ctx context.Context, exchange, routingKey, eventID string, event []byte) error {
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
	err := p.ch.PublishWithContext(confirmCtx, exchange, routingKey, true, false, amqp091.Publishing{
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
				p.failed = fmt.Errorf("event %s was unroutable: %s", eventID, returned.ReplyText)
				return p.failed
			}
		default:
		}
		return nil
	case <-confirmCtx.Done():
		p.failed = fmt.Errorf("await publisher confirmation for %s: %w", eventID, confirmCtx.Err())
		return p.failed
	}
}
