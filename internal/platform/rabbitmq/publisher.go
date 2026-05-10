package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

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
	// INJECT: OpenTelemetry writes the TraceID and SpanID into the headers map.
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
