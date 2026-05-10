package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// HandlerFunc defines the contract for our core business logic.
// The Consumer handles the broker; the Handler handles the database/API calls.
type HandlerFunc func(ctx context.Context, notificationID uuid.UUID) error

type Consumer struct {
	ch    *amqp.Channel
	queue string
}

func NewConsumer(ch *amqp.Channel) *Consumer {
	return &Consumer{ch: ch, queue: "notifications.main"}
}

// Start initiates a concurrent worker pool listening to the queue.
func (c *Consumer) Start(ctx context.Context, concurrency int, handler HandlerFunc) error {
	// 1. QoS (Quality of Service) - The ultimate backpressure mechanism.
	// This tells RabbitMQ to NEVER send more messages than our workers can handle at once.
	if err := c.ch.Qos(concurrency, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// 2. Consume with Manual Acknowledgements (autoAck = false)
	msgs, err := c.ch.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to register amqp consumer: %w", err)
	}

	// 3. Spawn the concurrent worker pool
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					slog.Info("shutting down amqp worker", "worker_id", workerID)
					return
				case msg, ok := <-msgs:
					if !ok {
						slog.Warn("amqp channel closed")
						return
					}
					c.processMessage(msg, handler)
				}
			}
		}(i)
	}
	return nil
}

func (c *Consumer) processMessage(msg amqp.Delivery, handler HandlerFunc) {
	// EXTRACT: Pull the TraceID out of the AMQP headers.
	// We pass context.Background() as the base, because the true context is reconstructed from the headers.
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), amqpTableCarrier(msg.Headers))
	ctx, span := otel.Tracer("rabbitmq").Start(ctx, "ConsumeFromQueue", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	var payload NotificationMessage
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		slog.ErrorContext(ctx, "poison pill detected, dropping message", "error", err)
		// Nack without requeueing drops it completely (or routes to a DLQ if configured)
		msg.Nack(false, false)
		return
	}

	// Hand over to the business logic orchestrator
	err := handler(ctx, payload.ID)
	if err != nil {
		slog.ErrorContext(ctx, "handler failed, message requeued", "error", err, "id", payload.ID)
		// If our rate-limiter blocked it, or the API crashed, put it back in the queue
		msg.Nack(false, true)
		return
	}

	// Success! Safely remove from RabbitMQ.
	msg.Ack(false)
}
