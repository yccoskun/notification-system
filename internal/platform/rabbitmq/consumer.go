package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

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
					processDelivery(msg, handler)
				}
			}
		}(i)
	}
	return nil
}

func processDelivery(msg amqp.Delivery, handler HandlerFunc) {
	// EXTRACT: Pull the TraceID out of the AMQP headers.
	// We pass context.Background() as the base, because the true context is reconstructed from the headers.
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), amqpTableCarrier(msg.Headers))
	ctx, span := otel.Tracer("rabbitmq").Start(ctx, "ConsumeFromQueue", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	var payload NotificationMessage
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		slog.ErrorContext(ctx, "poison pill detected, dropping message", "error", err)
		// Nack without requeueing drops it completely (or routes to a DLQ if configured)
		if err := msg.Nack(false, false); err != nil {
			slog.Error("failed to nack poison pill", "error", err)
		}
		return
	}

	// Hand over to the business logic orchestrator
	err := handler(ctx, payload.ID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			slog.WarnContext(ctx, "dropping orphan message: notification not in DB", "id", payload.ID)
			if err := msg.Ack(false); err != nil {
				slog.Error("failed to ack message", "error", err)
			}
			return
		}
		slog.ErrorContext(ctx, "handler failed, message requeued", "error", err, "id", payload.ID)
		// If our rate-limiter blocked it, or the API crashed, put it back in the queue
		if err := msg.Nack(false, true); err != nil {
			slog.Error("failed to nack/requeue", "error", err)
		}
		return
	}

	// Success! Safely remove from RabbitMQ.
	if err := msg.Ack(false); err != nil {
		slog.Error("failed to ack message", "error", err)
	}
}

// RunResilientConsumer maintains a long-lived consumer session: it reconnects
// automatically when the broker or TCP session drops.
func RunResilientConsumer(ctx context.Context, url string, concurrency int, handler HandlerFunc) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		conn, ch, err := NewChannel(url)
		if err != nil {
			slog.Warn("rabbitmq consumer dial failed", "error", err)
			sleepUntil(ctx, 2*time.Second)
			continue
		}

		if err := SetupTopology(ch); err != nil {
			slog.Warn("rabbitmq consumer topology failed", "error", err)
			_ = ch.Close()
			_ = conn.Close()
			sleepUntil(ctx, 2*time.Second)
			continue
		}

		err = runConsumerSession(ctx, conn, ch, concurrency, handler)
		_ = ch.Close()
		_ = conn.Close()

		if ctx.Err() != nil {
			return ctx.Err()
		}

		slog.Warn("rabbitmq consumer session ended, reconnecting", "error", err)
		sleepUntil(ctx, 2*time.Second)
	}
}

func runConsumerSession(ctx context.Context, conn *amqp.Connection, ch *amqp.Channel, concurrency int, handler HandlerFunc) error {
	if err := ch.Qos(concurrency, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	msgs, err := ch.Consume("notifications.main", "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					slog.Info("shutting down amqp worker", "worker_id", workerID)
					return
				case msg, ok := <-msgs:
					if !ok {
						slog.Warn("amqp channel closed", "worker_id", workerID)
						return
					}
					processDelivery(msg, handler)
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		_ = conn.Close()
		wg.Wait()
		return ctx.Err()
	case <-done:
		return fmt.Errorf("delivery channel closed")
	}
}

func sleepUntil(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
