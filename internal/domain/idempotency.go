package domain

import (
	"context"
	"time"
)

type IdempotencyGuard interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key string) error
}
