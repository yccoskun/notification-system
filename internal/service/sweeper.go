package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"notification-system/internal/domain"
)

// Publisher is a subset interface of our RabbitMQ publisher
type Publisher interface {
	Publish(ctx context.Context, notificationID uuid.UUID, priority int) error
}

type Sweeper struct {
	repo      domain.NotificationRepository
	publisher Publisher
	batchSize int
}

func NewSweeper(repo domain.NotificationRepository, publisher Publisher, batchSize int) *Sweeper {
	return &Sweeper{
		repo:      repo,
		publisher: publisher,
		batchSize: batchSize,
	}
}

// Start initiates the infinite background polling loop.
// It blocks until the context is canceled.
func (s *Sweeper) Start(ctx context.Context, interval time.Duration) {
	slog.Info("sweeper daemon started", "interval", interval, "batch_size", s.batchSize)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("sweeper daemon shutting down")
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Sweeper) sweep(ctx context.Context) {
	// 1. Fetch locked batch from DB
	notifications, err := s.repo.GetPendingForDelivery(ctx, s.batchSize)
	if err != nil {
		slog.ErrorContext(ctx, "sweeper failed to fetch pending notifications", "error", err)
		return
	}

	if len(notifications) == 0 {
		return // Nothing to do
	}

	slog.InfoContext(ctx, "sweeper recovered pending notifications", "count", len(notifications))

	// 2. Publish to RabbitMQ
	for _, n := range notifications {
		err := s.publisher.Publish(ctx, n.ID, n.Priority)
		if err != nil {
			slog.ErrorContext(ctx, "sweeper failed to publish message", "id", n.ID, "error", err)
			// Lease was advanced by GetPendingForDelivery (+5m). Publish failed (e.g. broker
			// down); reset send_at so the next sweep can retry—do not wait for the lease TTL.
			if retryErr := s.repo.ScheduleRetry(ctx, n.ID, time.Now()); retryErr != nil {
				slog.ErrorContext(ctx, "sweeper failed to reschedule after publish error", "id", n.ID, "error", retryErr)
			}
		}
	}
}
