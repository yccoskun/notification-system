package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Publisher implements domain.BrokerPublisher
type Publisher struct {
	ch         *amqp.Channel
	exchange   string
	routingKey string
}

func NewPublisher(ch *amqp.Channel) *Publisher {
	return &Publisher{
		ch:         ch,
		exchange:   "notifications.exchange",
		routingKey: "notifications.main",
	}
}

// NotificationMessage is the minimalist payload sent to the queue.
type NotificationMessage struct {
	ID       uuid.UUID `json:"id"`
	Priority int       `json:"priority"`
}

// Publish pushes a lightweight message to RabbitMQ with trace context injected.
func (p *Publisher) Publish(ctx context.Context, notificationID uuid.UUID, priority int) error {
	ctx, span := otel.Tracer("rabbitmq").Start(ctx, "PublishToQueue", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	msg := NotificationMessage{ID: notificationID, Priority: priority}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal rabbitmq message: %w", err)
	}

	headers := make(amqp.Table)
	otel.GetTextMapPropagator().Inject(ctx, amqpTableCarrier(headers))

	publishing := amqp.Publishing{
		Headers:      headers,
		ContentType:  "application/json",
		Body:         body,
		Priority:     uint8(priority),
		DeliveryMode: amqp.Persistent, // Guarantee durability to disk
	}

	err = p.ch.PublishWithContext(ctx, p.exchange, p.routingKey, false, false, publishing)
	if err != nil {
		return fmt.Errorf("failed to publish to amqp exchange: %w", err)
	}

	return nil
}

// ResilientPublisher wraps a RabbitMQ publisher with automatic reconnect on
// publish failures (e.g. broker restart or stale TCP connection).
type ResilientPublisher struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewResilientPublisher dials RabbitMQ, declares topology, and returns a
// publisher safe to use across broker restarts.
func NewResilientPublisher(url string) (*ResilientPublisher, error) {
	p := &ResilientPublisher{url: url}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.connectLocked(); err != nil {
		return nil, err
	}
	return p, nil
}

// Close releases the broker connection.
func (p *ResilientPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.teardownLocked()
	return nil
}

// Publish implements domain.BrokerPublisher. On failure it tears down the
// session, reconnects once, and retries the publish.
func (p *ResilientPublisher) Publish(ctx context.Context, notificationID uuid.UUID, priority int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	try := func() error {
		if p.ch == nil {
			if err := p.connectLocked(); err != nil {
				return err
			}
		}
		pub := NewPublisher(p.ch)
		return pub.Publish(ctx, notificationID, priority)
	}

	err := try()
	if err == nil {
		return nil
	}
	firstErr := err

	p.teardownLocked()
	if err := p.connectLocked(); err != nil {
		return fmt.Errorf("%w: reconnect: %v", firstErr, err)
	}
	pub := NewPublisher(p.ch)
	if err := pub.Publish(ctx, notificationID, priority); err != nil {
		return fmt.Errorf("%w: retry after reconnect: %v", firstErr, err)
	}
	return nil
}

func (p *ResilientPublisher) connectLocked() error {
	conn, ch, err := NewChannel(p.url)
	if err != nil {
		return fmt.Errorf("rabbitmq dial: %w", err)
	}
	if err := SetupTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("rabbitmq topology: %w", err)
	}
	p.conn = conn
	p.ch = ch
	return nil
}

func (p *ResilientPublisher) teardownLocked() {
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}
