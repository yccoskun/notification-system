package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

	// 1. Spin up ephemeral PostgreSQL container using the modernized API
	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("notification_test"),
		postgres.WithUsername("admin"),
		postgres.WithPassword("secret"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	// 2. Apply Migrations (Simulated for reliable test execution)
	// In a real pipeline, we'd use golang-migrate, but executing the schema directly guarantees no pathing errors.
	schemaPath := filepath.Join("..", "..", "..", "migrations", "000001_init_schema.up.sql")
	schemaBytes, err := os.ReadFile(schemaPath)
	require.NoError(t, err, "Migration file must exist at %s", schemaPath)

	_, err = pool.Exec(ctx, string(schemaBytes))
	require.NoError(t, err, "Failed to execute schema")

	// Teardown closure
	teardown := func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}

	return pool, teardown
}

func TestNotificationRepository_CreateBatch(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	t.Run("successfully inserts 1000 records and ignores duplicates", func(t *testing.T) {
		batchSize := 1000
		notifications := make([]*domain.Notification, batchSize)
		batchID := uuid.New()

		for i := 0; i < batchSize; i++ {
			idempKey := fmt.Sprintf("idemp-key-%d", i)
			notifications[i] = &domain.Notification{
				ID:             uuid.New(),
				BatchID:        &batchID,
				Recipient:      fmt.Sprintf("+1555000%04d", i),
				Channel:        domain.ChannelSMS,
				Priority:       5,
				Status:         domain.StatusPending,
				IdempotencyKey: &idempKey,
				SendAt:         time.Now(),
			}
		}

		// 1. First Insert - Should succeed entirely
		err := repo.CreateBatch(ctx, notifications)
		require.NoError(t, err)

		// Verify count
		var count int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM notifications").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, batchSize, count)

		// 2. Second Insert (Duplicate Idempotency Keys) - Should succeed but insert 0 new rows
		err = repo.CreateBatch(ctx, notifications)
		require.NoError(t, err)

		// Verify count remains the same (ON CONFLICT DO NOTHING worked)
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM notifications").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, batchSize, count)
	})
}

func TestNotificationRepository_GetPendingForDelivery_Concurrency(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	// Seed 100 pending notifications
	var notifications []*domain.Notification
	for i := 0; i < 100; i++ {
		notifications = append(notifications, &domain.Notification{
			ID:        uuid.New(),
			Recipient: "test@example.com",
			Channel:   domain.ChannelEmail,
			Priority:  1,
			Status:    domain.StatusPending,
			SendAt:    time.Now().Add(-1 * time.Minute), // Due in the past
		})
	}
	require.NoError(t, repo.CreateBatch(ctx, notifications))

	t.Run("SKIP LOCKED prevents race conditions across concurrent workers", func(t *testing.T) {
		workerCount := 5
		fetchPerWorker := 20

		var wg sync.WaitGroup
		var mu sync.Mutex

		// Map to track unique IDs fetched across all workers
		fetchedIDs := make(map[uuid.UUID]bool)
		totalFetched := 0

		// Fire 5 goroutines at the exact same time
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Each worker attempts to pull 20 rows
				items, err := repo.GetPendingForDelivery(ctx, fetchPerWorker)
				require.NoError(t, err)

				mu.Lock()
				defer mu.Unlock()

				totalFetched += len(items)
				for _, item := range items {
					// If this ID is already in the map, SKIP LOCKED failed!
					assert.False(t, fetchedIDs[item.ID], "Duplicate row fetched! SKIP LOCKED failed.")
					fetchedIDs[item.ID] = true
				}
			}()
		}

		wg.Wait()

		// Verify that exactly 100 unique records were fetched across all 5 concurrent workers
		assert.Equal(t, 100, totalFetched, "Expected exactly 100 rows to be processed")
		assert.Equal(t, 100, len(fetchedIDs), "Expected exactly 100 unique IDs")
	})
}

func TestNotificationRepository_ScheduleRetry(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	repo := mypostgres.NewNotificationRepository(pool)
	ctx := context.Background()

	t.Run("successfully pushes send_at into the future", func(t *testing.T) {
		// 1. Seed a test notification
		id := uuid.New()
		initialTime := time.Now().Round(time.Microsecond) // Match Postgres precision

		notification := &domain.Notification{
			ID:        id,
			Recipient: "retry-test@example.com",
			Channel:   domain.ChannelEmail,
			Priority:  5,
			Status:    domain.StatusPending,
			SendAt:    initialTime,
		}

		err := repo.CreateBatch(ctx, []*domain.Notification{notification})
		require.NoError(t, err)

		// 2. Schedule the retry for exactly 1 hour in the future
		futureTime := time.Now().Add(1 * time.Hour).Round(time.Microsecond)
		err = repo.ScheduleRetry(ctx, id, futureTime)
		require.NoError(t, err)

		// 3. Fetch and Verify
		updatedNotif, err := repo.GetByID(ctx, id)
		require.NoError(t, err)

		// We use WithinDuration to prevent flaky tests due to microsecond rounding differences between Go and Postgres
		assert.WithinDuration(t, futureTime, updatedNotif.SendAt, 1*time.Millisecond, "SendAt should be updated to the future time")
		assert.True(t, updatedNotif.UpdatedAt.After(updatedNotif.CreatedAt), "UpdatedAt should be modified")
	})

	t.Run("returns error when notification does not exist", func(t *testing.T) {
		err := repo.ScheduleRetry(ctx, uuid.New(), time.Now())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "notification not found for scheduling")
	})
}
