package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"notification-system/internal/api"
	"notification-system/internal/domain"
)

type countingPublisher struct {
	mu sync.Mutex
	n  int
}

func (c *countingPublisher) Publish(ctx context.Context, notificationID uuid.UUID, priority int) error {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return nil
}

func (c *countingPublisher) publishCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

type scheduleRepo struct {
	last []*domain.Notification
}

func (r *scheduleRepo) CreateBatch(_ context.Context, notifications []*domain.Notification) ([]uuid.UUID, error) {
	r.last = append([]*domain.Notification(nil), notifications...)
	out := make([]uuid.UUID, len(notifications))
	for i, n := range notifications {
		out[i] = n.ID
	}
	return out, nil
}

func (r *scheduleRepo) GetByID(context.Context, uuid.UUID) (*domain.Notification, error) {
	panic("unimplemented")
}
func (r *scheduleRepo) GetByBatchID(context.Context, uuid.UUID) ([]*domain.Notification, error) {
	panic("unimplemented")
}
func (r *scheduleRepo) List(context.Context, domain.NotificationListFilter, int, int) ([]*domain.Notification, int64, error) {
	panic("unimplemented")
}
func (r *scheduleRepo) UpdateStatus(context.Context, uuid.UUID, domain.NotificationStatus, int, *string) error {
	panic("unimplemented")
}
func (r *scheduleRepo) GetPendingForDelivery(context.Context, int) ([]*domain.Notification, error) {
	panic("unimplemented")
}
func (r *scheduleRepo) ScheduleRetry(context.Context, uuid.UUID, time.Time) error { panic("unimplemented") }

func TestHandleCreate_futureSendAtSkipsBrokerPublish(t *testing.T) {
	gin.SetMode(gin.TestMode)
	future := time.Now().Add(2 * time.Hour).UTC()
	body := map[string]any{
		"idempotency_key": "sched-1",
		"recipient":       "+15550001111",
		"channel":         "SMS",
		"payload":         map[string]any{"message": "hi"},
		"send_at":         future.Format(time.RFC3339Nano),
	}
	b, _ := json.Marshal(body)

	repo := &scheduleRepo{}
	pub := &countingPublisher{}
	h := api.NewNotificationHandler(repo, pub)
	r := gin.New()
	r.POST("/notifications", h.HandleCreate)

	req := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, 0, pub.publishCount())
	require.Len(t, repo.last, 1)
	require.WithinDuration(t, future, repo.last[0].SendAt, time.Second)
}

func TestHandleCreate_immediateSendAtPublishes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	past := time.Now().Add(-1 * time.Minute).UTC()
	body := map[string]any{
		"idempotency_key": "sched-2",
		"recipient":       "+15550002222",
		"channel":         "SMS",
		"payload":         map[string]any{"message": "hi"},
		"send_at":         past.Format(time.RFC3339Nano),
	}
	b, _ := json.Marshal(body)

	repo := &scheduleRepo{}
	pub := &countingPublisher{}
	h := api.NewNotificationHandler(repo, pub)
	r := gin.New()
	r.POST("/notifications", h.HandleCreate)

	req := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, 1, pub.publishCount())
}

func TestHandleCreate_omitSendAtPublishes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := map[string]any{
		"idempotency_key": "sched-3",
		"recipient":       "+15550003333",
		"channel":         "SMS",
		"payload":         map[string]any{"message": "hi"},
	}
	b, _ := json.Marshal(body)

	repo := &scheduleRepo{}
	pub := &countingPublisher{}
	h := api.NewNotificationHandler(repo, pub)
	r := gin.New()
	r.POST("/notifications", h.HandleCreate)

	req := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, 1, pub.publishCount())
}

func TestHandleBatchSubmit_mixedSchedulePublishCount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	future := time.Now().Add(3 * time.Hour).UTC()
	body := map[string]any{
		"idempotency_key": "batch-sched",
		"notifications": []map[string]any{
			{
				"recipient": "+15550004444",
				"channel":   "SMS",
				"payload":   map[string]any{"message": "a"},
				"send_at":   future.Format(time.RFC3339Nano),
			},
			{
				"recipient": "+15550005555",
				"channel":   "SMS",
				"payload":   map[string]any{"message": "b"},
			},
		},
	}
	b, _ := json.Marshal(body)

	repo := &scheduleRepo{}
	pub := &countingPublisher{}
	h := api.NewNotificationHandler(repo, pub)
	r := gin.New()
	r.POST("/batch", h.HandleBatchSubmit)

	req := httptest.NewRequest(http.MethodPost, "/batch", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, 1, pub.publishCount(), "only the row without future send_at should fast-publish")
	require.Len(t, repo.last, 2)
}

func TestHandleCreate_sendAtTooFarFuture400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	far := time.Now().Add(api.MaxScheduleHorizon + 48*time.Hour).UTC()
	body := map[string]any{
		"idempotency_key": "sched-bad",
		"recipient":       "+15550006666",
		"channel":         "SMS",
		"payload":         map[string]any{"message": "hi"},
		"send_at":         far.Format(time.RFC3339Nano),
	}
	b, _ := json.Marshal(body)

	repo := &scheduleRepo{}
	pub := &countingPublisher{}
	h := api.NewNotificationHandler(repo, pub)
	r := gin.New()
	r.POST("/notifications", h.HandleCreate)

	req := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, 0, pub.publishCount())
	require.Contains(t, w.Body.String(), "send_at")
}

func TestHandleCreate_responseIncludesSendAt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	when := time.Now().Add(45 * time.Minute).Round(time.Second).UTC()
	body := map[string]any{
		"idempotency_key": "sched-resp",
		"recipient":       "+15550007777",
		"channel":         "SMS",
		"payload":         map[string]any{"message": "hi"},
		"send_at":         when.Format(time.RFC3339Nano),
	}
	b, _ := json.Marshal(body)

	repo := &scheduleRepo{}
	h := api.NewNotificationHandler(repo, &countingPublisher{})
	r := gin.New()
	r.POST("/notifications", h.HandleCreate)

	req := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	raw, ok := resp["send_at"].(string)
	require.True(t, ok, "send_at in response: %v", resp)
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err)
	require.WithinDuration(t, when, parsed, time.Second)
}