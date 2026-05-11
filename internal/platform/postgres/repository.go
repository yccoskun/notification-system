package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notification-system/internal/domain"
)

type NotificationRepository struct {
	db *pgxpool.Pool
}

func NewNotificationRepository(db *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{db: db}
}

// MustConnect is a helper for main.go to ensure we have a DB or crash early.
func MustConnect(connStr string) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		panic(fmt.Sprintf("unable to connect to database: %v", err))
	}

	if err := pool.Ping(ctx); err != nil {
		panic(fmt.Sprintf("database ping failed: %v", err))
	}
	return pool
}

// CreateBatch inserts a high-throughput batch of notifications efficiently.
func (r *NotificationRepository) CreateBatch(ctx context.Context, notifications []*domain.Notification) ([]uuid.UUID, error) {
	batch := &pgx.Batch{}

	query := `
		INSERT INTO notifications (
			id, batch_id, recipient, channel, template_id, payload, priority, 
			status, idempotency_key, send_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`

	for _, n := range notifications {
		batch.Queue(query, n.ID, n.BatchID, n.Recipient, n.Channel, n.TemplateID, n.Payload,
			n.Priority, n.Status, n.IdempotencyKey, n.SendAt)
	}

	results := r.db.SendBatch(ctx, batch)
	defer results.Close()

	insertedIDs := make([]uuid.UUID, 0, len(notifications))
	for i := 0; i < len(notifications); i++ {
		var id uuid.UUID
		err := results.QueryRow().Scan(&id)
		if err != nil {
			if err == pgx.ErrNoRows {
				// This row was a duplicate, ignore it
				continue
			}
			return nil, fmt.Errorf("failed to insert batch row %d: %w", i, err)
		}
		insertedIDs = append(insertedIDs, id)
	}
	return insertedIDs, nil
}

// GetByID fetches a single notification for status checks.
func (r *NotificationRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	query := `
		SELECT id, batch_id, recipient, channel, template_id, payload, priority, 
		       status, idempotency_key, retry_count, last_error, send_at, created_at, updated_at
		FROM notifications WHERE id = $1`

	var n domain.Notification
	err := r.db.QueryRow(ctx, query, id).Scan(
		&n.ID, &n.BatchID, &n.Recipient, &n.Channel, &n.TemplateID, &n.Payload, &n.Priority,
		&n.Status, &n.IdempotencyKey, &n.RetryCount, &n.LastError, &n.SendAt, &n.CreatedAt, &n.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("notification not found")
		}
		return nil, fmt.Errorf("failed to fetch notification: %w", err)
	}
	return &n, nil
}

// UpdateStatus records the outcome of a delivery attempt.
func (r *NotificationRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.NotificationStatus, retryCount int, lastErr *string) error {
	query := `
		UPDATE notifications 
		SET status = $1, retry_count = $2, last_error = $3, updated_at = NOW() 
		WHERE id = $4`

	tag, err := r.db.Exec(ctx, query, status, retryCount, lastErr, id)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("notification not found for update")
	}
	return nil
}

// GetPendingForDelivery atomically fetches pending messages and pushes their send_at
// timestamp 5 minutes into the future to act as a distributed lease lock.
func (r *NotificationRepository) GetPendingForDelivery(ctx context.Context, batchSize int) ([]*domain.Notification, error) {
	query := `
		WITH locked_rows AS (
			SELECT id 
			FROM notifications 
			WHERE status = 'PENDING' AND send_at <= NOW()
			ORDER BY priority DESC, created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE notifications n
		SET send_at = NOW() + INTERVAL '5 minutes', updated_at = NOW()
		FROM locked_rows lr
		WHERE n.id = lr.id
		RETURNING n.id, n.batch_id, n.recipient, n.channel, n.template_id, n.payload, 
		          n.priority, n.status, n.idempotency_key, n.retry_count, 
		          n.last_error, n.send_at, n.created_at, n.updated_at;
	`

	rows, err := r.db.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending notifications: %w", err)
	}
	defer rows.Close()

	var notifications []*domain.Notification
	for rows.Next() {
		var n domain.Notification
		err := rows.Scan(
			&n.ID, &n.BatchID, &n.Recipient, &n.Channel, &n.TemplateID, &n.Payload,
			&n.Priority, &n.Status, &n.IdempotencyKey, &n.RetryCount,
			&n.LastError, &n.SendAt, &n.CreatedAt, &n.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan notification: %w", err)
		}
		notifications = append(notifications, &n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return notifications, nil
}

// ScheduleRetry pushes the send_at timestamp into the future for exponential backoff.
func (r *NotificationRepository) ScheduleRetry(ctx context.Context, id uuid.UUID, sendAt time.Time) error {
	query := `
		UPDATE notifications 
		SET send_at = $1, updated_at = NOW() 
		WHERE id = $2`

	tag, err := r.db.Exec(ctx, query, sendAt, id)
	if err != nil {
		return fmt.Errorf("failed to schedule retry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("notification not found for scheduling")
	}
	return nil
}
