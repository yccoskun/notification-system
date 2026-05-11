package postgres_test

import (
	"context"
	"fmt"
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
	templateID := uuid.New()
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

	// FIX: Capture insertedIDs and verify length
	insertedIDs, err := repo.CreateBatch(ctx, []*domain.Notification{n})
	require.NoError(t, err)
	assert.Len(t, insertedIDs, 1)
	assert.Equal(t, id, insertedIDs[0])

	fetched, err := repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, fetched.ID)
	assert.Equal(t, templateID, *fetched.TemplateID)
}

func TestNotificationRepository_GetByBatchID(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	batchID := uuid.New()
	now := time.Now().Round(time.Microsecond)
	otherBatch := uuid.New()

	n1 := &domain.Notification{
		ID:             uuid.New(),
		BatchID:        &batchID,
		Recipient:      "a@example.com",
		Channel:        domain.ChannelEmail,
		Payload:        map[string]any{"k": 1},
		Priority:       5,
		Status:         domain.StatusPending,
		IdempotencyKey: stringPtr("batch-test-a"),
		SendAt:         now,
	}
	n2 := &domain.Notification{
		ID:             uuid.New(),
		BatchID:        &batchID,
		Recipient:      "b@example.com",
		Channel:        domain.ChannelEmail,
		Payload:        map[string]any{"k": 2},
		Priority:       3,
		Status:         domain.StatusPending,
		IdempotencyKey: stringPtr("batch-test-b"),
		SendAt:         now,
	}
	nOther := &domain.Notification{
		ID:             uuid.New(),
		BatchID:        &otherBatch,
		Recipient:      "other@example.com",
		Channel:        domain.ChannelEmail,
		Payload:        map[string]any{},
		Priority:       1,
		Status:         domain.StatusPending,
		IdempotencyKey: stringPtr("batch-test-other"),
		SendAt:         now,
	}

	_, err := repo.CreateBatch(ctx, []*domain.Notification{n1, n2, nOther})
	require.NoError(t, err)

	list, err := repo.GetByBatchID(ctx, batchID)
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.ElementsMatch(t, []uuid.UUID{n1.ID, n2.ID}, []uuid.UUID{list[0].ID, list[1].ID})
	for _, row := range list {
		assert.Equal(t, batchID, *row.BatchID)
	}

	empty, err := repo.GetByBatchID(ctx, uuid.New())
	require.NoError(t, err)
	assert.Empty(t, empty)
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

	// FIX: Handle the new return value
	_, err := repo.CreateBatch(ctx, []*domain.Notification{n})
	require.NoError(t, err)

	errMsg := "provider timeout"
	err = repo.UpdateStatus(ctx, id, domain.StatusFailed, 1, &errMsg)
	require.NoError(t, err)

	fetched, err := repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusFailed, fetched.Status)
	assert.Equal(t, 1, fetched.RetryCount)
	assert.Equal(t, errMsg, *fetched.LastError)
}

func TestNotificationRepository_UpdateStatusCancelled(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	id := uuid.New()
	n := &domain.Notification{
		ID:             id,
		Recipient:      "+15550009999",
		Channel:        domain.ChannelSMS,
		Payload:        map[string]any{},
		Priority:       1,
		Status:         domain.StatusPending,
		IdempotencyKey: stringPtr("cancel-repo-test"),
		SendAt:         time.Now(),
	}

	_, err := repo.CreateBatch(ctx, []*domain.Notification{n})
	require.NoError(t, err)

	reason := "cancelled by user via API"
	err = repo.UpdateStatus(ctx, id, domain.StatusCancelled, 0, &reason)
	require.NoError(t, err)

	fetched, err := repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, fetched.Status)
	assert.Equal(t, reason, *fetched.LastError)
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

	// FIX: Handle the new return value
	_, err := repo.CreateBatch(ctx, []*domain.Notification{n})
	require.NoError(t, err)

	futureTime := time.Now().Add(1 * time.Hour).Round(time.Microsecond)
	err = repo.ScheduleRetry(ctx, id, futureTime)
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

	// FIX: Verify all 100 were actually inserted
	insertedIDs, err := repo.CreateBatch(ctx, notifications)
	require.NoError(t, err)
	assert.Len(t, insertedIDs, 100)

	t.Run("CTE SKIP LOCKED prevents race conditions", func(t *testing.T) {
		// ... rest of test remains the same ...
	})
}

func TestNotificationRepository_Idempotency(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	key := "duplicate-key-99"
	n1 := &domain.Notification{ID: uuid.New(), IdempotencyKey: &key, Recipient: "a", Channel: "SMS", Payload: map[string]any{}, SendAt: time.Now()}
	n2 := &domain.Notification{ID: uuid.New(), IdempotencyKey: &key, Recipient: "a", Channel: "SMS", Payload: map[string]any{}, SendAt: time.Now()}

	// First insert
	inserted1, err := repo.CreateBatch(ctx, []*domain.Notification{n1})
	require.NoError(t, err)
	assert.Len(t, inserted1, 1)

	// Second insert (duplicate key)
	inserted2, err := repo.CreateBatch(ctx, []*domain.Notification{n2})
	require.NoError(t, err)

	// PROOF: inserted2 should be empty because of ON CONFLICT DO NOTHING
	assert.Empty(t, inserted2)
}

func stringPtr(s string) *string { return &s }
