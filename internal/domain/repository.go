package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// NotificationListFilter restricts rows returned by NotificationRepository.List.
// All fields are optional; unset fields are not applied.
type NotificationListFilter struct {
	Status      *NotificationStatus
	Channel     *ChannelType
	CreatedFrom *time.Time // inclusive lower bound on created_at
	CreatedTo   *time.Time // inclusive upper bound on created_at
}

type NotificationRepository interface {
	CreateBatch(ctx context.Context, notifications []*Notification) ([]uuid.UUID, error)
	GetByID(ctx context.Context, id uuid.UUID) (*Notification, error)
	GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*Notification, error)
	List(ctx context.Context, filter NotificationListFilter, limit, offset int) ([]*Notification, int64, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status NotificationStatus, retryCount int, lastErr *string) error
	GetPendingForDelivery(ctx context.Context, batchSize int) ([]*Notification, error)
	ScheduleRetry(ctx context.Context, id uuid.UUID, sendAt time.Time) error
}

type BrokerPublisher interface {
	Publish(ctx context.Context, notificationID uuid.UUID, priority int) error
}
