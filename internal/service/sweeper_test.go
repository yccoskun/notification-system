package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"notification-system/internal/domain"
)

type sweepRepoStub struct {
	pending   []*domain.Notification
	scheduled map[uuid.UUID]time.Time
}

func (s *sweepRepoStub) CreateBatch(context.Context, []*domain.Notification) ([]uuid.UUID, error) {
	panic("unimplemented")
}

func (s *sweepRepoStub) GetByID(context.Context, uuid.UUID) (*domain.Notification, error) {
	panic("unimplemented")
}

func (s *sweepRepoStub) GetByBatchID(context.Context, uuid.UUID) ([]*domain.Notification, error) {
	panic("unimplemented")
}

func (s *sweepRepoStub) List(context.Context, domain.NotificationListFilter, int, int) ([]*domain.Notification, int64, error) {
	panic("unimplemented")
}

func (s *sweepRepoStub) UpdateStatus(context.Context, uuid.UUID, domain.NotificationStatus, int, *string) error {
	panic("unimplemented")
}

func (s *sweepRepoStub) GetPendingForDelivery(context.Context, int) ([]*domain.Notification, error) {
	out := s.pending
	s.pending = nil
	return out, nil
}

func (s *sweepRepoStub) ScheduleRetry(_ context.Context, id uuid.UUID, sendAt time.Time) error {
	if s.scheduled == nil {
		s.scheduled = make(map[uuid.UUID]time.Time)
	}
	s.scheduled[id] = sendAt
	return nil
}

type failPublisher struct{}

func (failPublisher) Publish(context.Context, uuid.UUID, int) error {
	return errors.New("broker unreachable")
}

func TestSweeper_ScheduleRetryOnPublishFailure(t *testing.T) {
	id := uuid.New()
	repo := &sweepRepoStub{
		pending: []*domain.Notification{
			{ID: id, Status: domain.StatusPending, SendAt: time.Now().Add(-time.Minute)},
		},
	}
	sw := NewSweeper(repo, failPublisher{}, 10)

	sw.sweep(context.Background())

	require.Contains(t, repo.scheduled, id)
	require.WithinDuration(t, time.Now(), repo.scheduled[id], 2*time.Second)
}
