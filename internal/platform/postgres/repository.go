package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notification-system/internal/domain"
)

// NotificationRepository implements domain.NotificationRepository using pgx.
type NotificationRepository struct {
	db *pgxpool.Pool
}

// NewNotificationRepository creates a new instance of the repository.
func NewNotificationRepository(db *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{db: db}
}

// CreateBatch inserts up to 1000 notifications in a single network round-trip.
func (r *NotificationRepository) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	if len(notifications) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `
		INSERT INTO notifications (
			id, batch_id, recipient, channel, template_id, payload, 
			priority, status, idempotency_key, retry_count, last_error, send_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (idempotency_key) DO NOTHING;`

	for _, n := range notifications {
		batch.Queue(query,
			n.ID, n.BatchID, n.Recipient, n.Channel, n.TemplateID, n.Payload,
			n.Priority, n.Status, n.IdempotencyKey, n.RetryCount, n.LastError, n.SendAt,
		)
	}

	// SendBatch executes all queued statements.
	br := r.db.SendBatch(ctx, batch)
	defer br.Close()

	// We must iterate over the batch results to catch query-level errors.
	for i := 0; i < len(notifications); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("failed to insert notification at index %d: %w", i, err)
		}
	}

	return nil
}

// GetByID fetches a single notification payload for the Worker to process.
func (r *NotificationRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	query := `
		SELECT id, batch_id, recipient, channel, template_id, payload, 
		       priority, status, idempotency_key, retry_count, last_error, send_at, created_at, updated_at
		FROM notifications WHERE id = $1`

	var n domain.Notification
	err := r.db.QueryRow(ctx, query, id).Scan(
		&n.ID, &n.BatchID, &n.Recipient, &n.Channel, &n.TemplateID, &n.Payload,
		&n.Priority, &n.Status, &n.IdempotencyKey, &n.RetryCount, &n.LastError, &n.SendAt, &n.CreatedAt, &n.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("notification not found")
		}
		return nil, fmt.Errorf("failed to query notification: %w", err)
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
		return fmt.Errorf("failed to update notification status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("notification not found for update")
	}
	return nil
}

// GetPendingForDelivery safely pulls scheduled or orphaned messages for the Sweeper.
func (r *NotificationRepository) GetPendingForDelivery(ctx context.Context, batchSize int) ([]*domain.Notification, error) {
	// SKIP LOCKED is critical here to prevent Sweeper collisions.
	query := `
		SELECT id, batch_id, recipient, channel, template_id, payload, 
		       priority, status, idempotency_key, retry_count, last_error, send_at, created_at, updated_at
		FROM notifications 
		WHERE status = 'PENDING' AND send_at <= NOW()
		ORDER BY priority DESC, created_at ASC
		LIMIT $1 
		FOR UPDATE SKIP LOCKED`

	rows, err := r.db.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending notifications: %w", err)
	}
	defer rows.Close()

	var notifications []*domain.Notification
	for rows.Next() {
		var n domain.Notification
		if err := rows.Scan(
			&n.ID, &n.BatchID, &n.Recipient, &n.Channel, &n.TemplateID, &n.Payload,
			&n.Priority, &n.Status, &n.IdempotencyKey, &n.RetryCount, &n.LastError, &n.SendAt, &n.CreatedAt, &n.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan notification: %w", err)
		}
		notifications = append(notifications, &n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return notifications, nil
}
