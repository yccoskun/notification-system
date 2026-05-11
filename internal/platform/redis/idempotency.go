package redis

import (
	"context"
	"fmt"
	"time"

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

func (i *IdempotencyGuard) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	// Note: We use the key as provided or prefix it
	fullKey := fmt.Sprintf("idemp:worker:%s", key)
	success, err := i.client.SetNX(ctx, fullKey, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("failed to execute redis SETNX: %w", err)
	}
	return success, nil
}

func (i *IdempotencyGuard) Release(ctx context.Context, key string) error {
	fullKey := fmt.Sprintf("idemp:worker:%s", key)
	return i.client.Del(ctx, fullKey).Err()
}
