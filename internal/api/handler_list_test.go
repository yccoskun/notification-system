package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"notification-system/internal/api"
	"notification-system/internal/domain"
)

type listStubRepo struct {
	listFn func(ctx context.Context, filter domain.NotificationListFilter, limit, offset int) ([]*domain.Notification, int64, error)
}

func (r *listStubRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) ([]uuid.UUID, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *listStubRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *listStubRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	return nil, nil
}

func (r *listStubRepo) List(ctx context.Context, filter domain.NotificationListFilter, limit, offset int) ([]*domain.Notification, int64, error) {
	if r.listFn != nil {
		return r.listFn(ctx, filter, limit, offset)
	}
	return nil, 0, nil
}

func (r *listStubRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.NotificationStatus, retryCount int, lastErr *string) error {
	return fmt.Errorf("not implemented")
}

func (r *listStubRepo) GetPendingForDelivery(ctx context.Context, batchSize int) ([]*domain.Notification, error) {
	return nil, nil
}

func (r *listStubRepo) ScheduleRetry(ctx context.Context, id uuid.UUID, sendAt time.Time) error {
	return nil
}

func TestHandleList_invalidStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := api.NewNotificationHandler(&listStubRepo{}, noopPublisher{}, noopStatusPublisher{})
	r := gin.New()
	r.GET("/notifications", h.HandleList)

	req := httptest.NewRequest(http.MethodGet, "/notifications?status=UNKNOWN", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleList_invalidChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := api.NewNotificationHandler(&listStubRepo{}, noopPublisher{}, noopStatusPublisher{})
	r := gin.New()
	r.GET("/notifications", h.HandleList)

	req := httptest.NewRequest(http.MethodGet, "/notifications?channel=FAX", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleList_invalidDateOrder(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := api.NewNotificationHandler(&listStubRepo{}, noopPublisher{}, noopStatusPublisher{})
	r := gin.New()
	r.GET("/notifications", h.HandleList)

	req := httptest.NewRequest(http.MethodGet, "/notifications?created_from=2025-01-02T00:00:00Z&created_to=2025-01-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleList_passesFiltersAndPagination(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var gotFilter domain.NotificationListFilter
	var gotLimit, gotOffset int
	repo := &listStubRepo{
		listFn: func(ctx context.Context, filter domain.NotificationListFilter, limit, offset int) ([]*domain.Notification, int64, error) {
			gotFilter = filter
			gotLimit = limit
			gotOffset = offset
			return []*domain.Notification{}, 0, nil
		},
	}
	h := api.NewNotificationHandler(repo, noopPublisher{}, noopStatusPublisher{})
	r := gin.New()
	r.GET("/notifications", h.HandleList)

	req := httptest.NewRequest(http.MethodGet, "/notifications?status=PENDING&channel=EMAIL&created_from=2025-06-01T10:00:00Z&created_to=2025-06-30T23:00:00Z&limit=25&offset=5", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotFilter.Status)
	assert.Equal(t, domain.StatusPending, *gotFilter.Status)
	require.NotNil(t, gotFilter.Channel)
	assert.Equal(t, domain.ChannelEmail, *gotFilter.Channel)
	require.NotNil(t, gotFilter.CreatedFrom)
	assert.Equal(t, 2025, gotFilter.CreatedFrom.UTC().Year())
	require.NotNil(t, gotFilter.CreatedTo)
	assert.Equal(t, 25, gotLimit)
	assert.Equal(t, 5, gotOffset)
}
