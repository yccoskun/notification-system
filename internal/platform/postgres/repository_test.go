package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"notification-system/internal/domain"
	mypostgres "notification-system/internal/platform/postgres"
)

func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	// 1. Ephemeral Postgres Container (Using modern postgres.Run API)
	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp")),
	)
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	// 3. Migrate Schema (template_id is now UUID)
	_, err = pool.Exec(ctx, `
		CREATE TABLE notifications (
			id UUID PRIMARY KEY,
			batch_id UUID,
			recipient VARCHAR(255) NOT NULL,
			channel VARCHAR(50) NOT NULL,
			template_id UUID,
			payload JSONB NOT NULL,
			priority INT NOT NULL,
			status VARCHAR(50) NOT NULL,
			idempotency_key VARCHAR(255) UNIQUE,
			retry_count INT DEFAULT 0,
			last_error TEXT,
			send_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		);
	`)
	require.NoError(t, err)

	teardown := func() {
		pool.Close()
		_ = container.Terminate(ctx)
	}

	return pool, teardown
}

func TestNotificationRepository_CreateAndGetByID(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	id := uuid.New()
	idempKey := "crud-test-key"
	templateID := uuid.New() // Fix: Using uuid.UUID instead of string
	now := time.Now().Round(time.Microsecond)

	n := &domain.Notification{
		ID:             id,
		Recipient:      "test@example.com",
		Channel:        domain.ChannelEmail,
		TemplateID:     &templateID,
		Payload:        map[string]any{"first_name": "Alice"},
		Priority:       5,
		Status:         domain.StatusPending,
		IdempotencyKey: &idempKey,
		SendAt:         now,
	}

	err := repo.CreateBatch(ctx, []*domain.Notification{n})
	require.NoError(t, err)

	fetched, err := repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, fetched.ID)
	assert.Equal(t, templateID, *fetched.TemplateID)
}

func TestNotificationRepository_UpdateStatus(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	id := uuid.New()
	n := &domain.Notification{
		ID:        id,
		Recipient: "+15550001234",
		Channel:   domain.ChannelSMS,
		Payload:   map[string]any{"msg": "hi"},
		Priority:  1,
		Status:    domain.StatusPending,
		SendAt:    time.Now(),
	}

	require.NoError(t, repo.CreateBatch(ctx, []*domain.Notification{n}))

	errMsg := "provider timeout"
	err := repo.UpdateStatus(ctx, id, domain.StatusFailed, 1, &errMsg)
	require.NoError(t, err)

	fetched, err := repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusFailed, fetched.Status)
	assert.Equal(t, 1, fetched.RetryCount)
	assert.Equal(t, errMsg, *fetched.LastError)
}

func TestNotificationRepository_ScheduleRetry(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	id := uuid.New()
	n := &domain.Notification{
		ID:        id,
		Recipient: "retry@example.com",
		Channel:   domain.ChannelEmail,
		Payload:   map[string]any{},
		Priority:  5,
		Status:    domain.StatusPending,
		SendAt:    time.Now().Round(time.Microsecond),
	}

	require.NoError(t, repo.CreateBatch(ctx, []*domain.Notification{n}))

	futureTime := time.Now().Add(1 * time.Hour).Round(time.Microsecond)
	err := repo.ScheduleRetry(ctx, id, futureTime)
	require.NoError(t, err)

	fetched, err := repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.WithinDuration(t, futureTime, fetched.SendAt, time.Millisecond)
}

func TestNotificationRepository_GetPendingForDelivery_Concurrency(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	var notifications []*domain.Notification
	for i := 0; i < 100; i++ {
		idemp := fmt.Sprintf("concurrency-test-%d", i)
		tid := uuid.New()
		notifications = append(notifications, &domain.Notification{
			ID:             uuid.New(),
			Recipient:      "test@example.com",
			Channel:        domain.ChannelEmail,
			TemplateID:     &tid,
			Priority:       1,
			Payload:        map[string]any{"data": "sync"},
			IdempotencyKey: &idemp,
			Status:         domain.StatusPending,
			SendAt:         time.Now().Add(-1 * time.Minute),
		})
	}
	require.NoError(t, repo.CreateBatch(ctx, notifications))

	t.Run("CTE SKIP LOCKED prevents race conditions across concurrent workers", func(t *testing.T) {
		workerCount := 5
		fetchPerWorker := 20

		var wg sync.WaitGroup
		var mu sync.Mutex

		fetchedIDs := make(map[uuid.UUID]bool)
		totalFetched := 0

		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				items, err := repo.GetPendingForDelivery(ctx, fetchPerWorker)
				require.NoError(t, err)

				mu.Lock()
				defer mu.Unlock()

				totalFetched += len(items)
				for _, item := range items {
					assert.False(t, fetchedIDs[item.ID], "Duplicate row fetched!")
					fetchedIDs[item.ID] = true
				}
			}()
		}

		wg.Wait()

		assert.Equal(t, 100, totalFetched)
		assert.Equal(t, 100, len(fetchedIDs))

		firstID := notifications[0].ID
		updatedRow, err := repo.GetByID(ctx, firstID)
		require.NoError(t, err)
		assert.True(t, updatedRow.SendAt.After(time.Now()))
	})
}
