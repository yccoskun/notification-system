package domain

import (
	"context"

	"github.com/google/uuid"
)

type NotificationRepository interface {
	CreateBatch(ctx context.Context, notifications []*Notification) error

	GetByID(ctx context.Context, id uuid.UUID) (*Notification, error)

	UpdateStatus(ctx context.Context, id uuid.UUID, status NotificationStatus, retryCount int, lastErr *string) error

	GetPendingForDelivery(ctx context.Context, batchSize int) ([]*Notification, error)
}

type TemplateRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Template, error)
	GetByName(ctx context.Context, name string) (*Template, error)
}

type BrokerPublisher interface {
	Publish(ctx context.Context, notificationID uuid.UUID, priority int) error
}

type RateLimiter interface {
	Allow(ctx context.Context, channel ChannelType) (bool, error)
}
