package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"notification-system/internal/api"
	"notification-system/internal/domain"
)

type noopPublisher struct{}

func (noopPublisher) Publish(ctx context.Context, notificationID uuid.UUID, priority int) error {
	return nil
}

type cancelStubRepo struct {
	mu             sync.Mutex
	byID           map[uuid.UUID]domain.Notification
	lastUpdate     *domain.NotificationStatus
	lastUpdateErr  *string
	lastUpdateID   uuid.UUID
	lastRetryCount int
}

func newCancelStubRepo() *cancelStubRepo {
	return &cancelStubRepo{byID: make(map[uuid.UUID]domain.Notification)}
}

func (r *cancelStubRepo) seed(n domain.Notification) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[n.ID] = n
}

func (r *cancelStubRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.byID[id]
	if !ok {
		return nil, fmt.Errorf("notification not found")
	}
	out := n
	return &out, nil
}

func (r *cancelStubRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.NotificationStatus, retryCount int, lastErr *string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastUpdateID = id
	r.lastUpdate = &status
	r.lastUpdateErr = lastErr
	r.lastRetryCount = retryCount
	n, ok := r.byID[id]
	if !ok {
		return fmt.Errorf("notification not found for update")
	}
	n.Status = status
	n.RetryCount = retryCount
	n.LastError = lastErr
	r.byID[id] = n
	return nil
}

func (r *cancelStubRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) ([]uuid.UUID, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *cancelStubRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	return nil, nil
}

func (r *cancelStubRepo) List(ctx context.Context, filter domain.NotificationListFilter, limit, offset int) ([]*domain.Notification, int64, error) {
	return nil, 0, fmt.Errorf("not implemented")
}

func (r *cancelStubRepo) GetPendingForDelivery(ctx context.Context, batchSize int) ([]*domain.Notification, error) {
	return nil, nil
}

func (r *cancelStubRepo) ScheduleRetry(ctx context.Context, id uuid.UUID, sendAt time.Time) error {
	return nil
}

type captureStatusPublisher struct {
	mu       sync.Mutex
	publish  []string
	detail   []string
}

func (c *captureStatusPublisher) Publish(ctx context.Context, id, status string) error {
	c.mu.Lock()
	c.publish = append(c.publish, id+"|"+status)
	c.mu.Unlock()
	return nil
}

func (c *captureStatusPublisher) PublishWithDetail(ctx context.Context, id, status, detail string) error {
	c.mu.Lock()
	c.detail = append(c.detail, id+"|"+status+"|"+detail)
	c.mu.Unlock()
	return nil
}

func TestHandleCancel_BroadcastsCancelledToStatusChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	id := uuid.New()
	repo := newCancelStubRepo()
	repo.seed(domain.Notification{
		ID:        id,
		Recipient: "a@b.com",
		Channel:   domain.ChannelEmail,
		Payload:   map[string]any{},
		Priority:  5,
		Status:    domain.StatusPending,
		SendAt:    time.Now(),
	})

	statusPub := &captureStatusPublisher{}
	r := gin.New()
	h := api.NewNotificationHandler(repo, noopPublisher{}, statusPub)
	r.DELETE("/notifications/:id", h.HandleCancel)

	req := httptest.NewRequest(http.MethodDelete, "/notifications/"+id.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	statusPub.mu.Lock()
	defer statusPub.mu.Unlock()
	require.Contains(t, statusPub.publish, id.String()+"|"+string(domain.StatusCancelled))
}

func TestHandleCancel_PendingUsesCancelledStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)

	id := uuid.New()
	repo := newCancelStubRepo()
	repo.seed(domain.Notification{
		ID:        id,
		Recipient: "a@b.com",
		Channel:   domain.ChannelEmail,
		Payload:   map[string]any{},
		Priority:  5,
		Status:    domain.StatusPending,
		SendAt:    time.Now(),
	})

	r := gin.New()
	h := api.NewNotificationHandler(repo, noopPublisher{}, noopStatusPublisher{})
	r.DELETE("/notifications/:id", h.HandleCancel)

	req := httptest.NewRequest(http.MethodDelete, "/notifications/"+id.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "successfully cancelled")
	require.NotNil(t, repo.lastUpdate)
	assert.Equal(t, domain.StatusCancelled, *repo.lastUpdate)
	require.NotNil(t, repo.lastUpdateErr)
	assert.Equal(t, "cancelled by user via API", *repo.lastUpdateErr)
	assert.Equal(t, id, repo.lastUpdateID)
}

func TestHandleCancel_NotPendingConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)

	id := uuid.New()
	repo := newCancelStubRepo()
	repo.seed(domain.Notification{
		ID:        id,
		Recipient: "a@b.com",
		Channel:   domain.ChannelEmail,
		Payload:   map[string]any{},
		Priority:  5,
		Status:    domain.StatusSent,
		SendAt:    time.Now(),
	})

	r := gin.New()
	h := api.NewNotificationHandler(repo, noopPublisher{}, noopStatusPublisher{})
	r.DELETE("/notifications/:id", h.HandleCancel)

	req := httptest.NewRequest(http.MethodDelete, "/notifications/"+id.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusConflict, w.Code)
	assert.Nil(t, repo.lastUpdate)
}

func TestHandleCancel_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := newCancelStubRepo()
	r := gin.New()
	h := api.NewNotificationHandler(repo, noopPublisher{}, noopStatusPublisher{})
	r.DELETE("/notifications/:id", h.HandleCancel)

	req := httptest.NewRequest(http.MethodDelete, "/notifications/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleCancel_InvalidUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := newCancelStubRepo()
	r := gin.New()
	h := api.NewNotificationHandler(repo, noopPublisher{}, noopStatusPublisher{})
	r.DELETE("/notifications/:id", h.HandleCancel)

	req := httptest.NewRequest(http.MethodDelete, "/notifications/not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
