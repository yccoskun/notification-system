package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// IdempotencyGuard ensures a notification is processed exactly once by the worker pool.
type IdempotencyGuard struct {
	client *redis.Client
	ttl    time.Duration
}

func NewIdempotencyGuard(client *redis.Client) *IdempotencyGuard {
	return &IdempotencyGuard{
		client: client,
		ttl:    24 * time.Hour, // Keep lock history for 24 hours
	}
}

// CheckAndSet attempts to acquire an exclusive lock for this notification ID.
// Returns true if the lock was acquired (safe to process), false if already processed.
func (i *IdempotencyGuard) CheckAndSet(ctx context.Context, notificationID uuid.UUID) (bool, error) {
	key := fmt.Sprintf("idemp:worker:%s", notificationID.String())

	// SETNX (Set if Not eXists) is perfectly atomic.
	// We set the value to "1". The actual value doesn't matter, only the key's existence.
	success, err := i.client.SetNX(ctx, key, "1", i.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("failed to execute redis SETNX: %w", err)
	}

	return success, nil
}

// Clear allows a worker to remove the lock if the message fundamentally failed
// (e.g., 500 from provider) and needs to be completely re-processed later by the Sweeper.
func (i *IdempotencyGuard) Clear(ctx context.Context, notificationID uuid.UUID) error {
	key := fmt.Sprintf("idemp:worker:%s", notificationID.String())
	return i.client.Del(ctx, key).Err()
}
