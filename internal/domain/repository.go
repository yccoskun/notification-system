package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type NotificationRepository interface {
	CreateBatch(ctx context.Context, notifications []*Notification) error
	GetByID(ctx context.Context, id uuid.UUID) (*Notification, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status NotificationStatus, retryCount int, lastErr *string) error
	GetPendingForDelivery(ctx context.Context, batchSize int) ([]*Notification, error)
	ScheduleRetry(ctx context.Context, id uuid.UUID, sendAt time.Time) error
}

type BrokerPublisher interface {
	Publish(ctx context.Context, notificationID uuid.UUID, priority int) error
}
