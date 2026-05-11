package redis_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"

	"notification-system/internal/domain"
	myredis "notification-system/internal/platform/redis"
)

func setupTestRedis(t *testing.T) (*goredis.Client, func()) {
	ctx := context.Background()

	// 1. Spin up ephemeral Redis using the modernized Run API
	container, err := rediscontainer.Run(ctx, "redis:7.2-alpine")
	require.NoError(t, err)

	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)

	// 2. Connect the Go client
	client := goredis.NewClient(&goredis.Options{
		Addr: endpoint,
	})

	teardown := func() {
		client.Close()
		_ = container.Terminate(ctx)
	}

	return client, teardown
}

func TestRateLimiter_TokenBucket_Boundary(t *testing.T) {
	client, teardown := setupTestRedis(t)
	defer teardown()

	limiter := myredis.NewRateLimiter(client)
	ctx := context.Background()
	channel := domain.ChannelSMS

	t.Run("allows exactly 100 requests and blocks the 101st", func(t *testing.T) {
		recipient := "user-123"
		// 1. Drain the bucket (simulate 100 quick requests)
		for i := 0; i < 100; i++ {
			allowed, err := limiter.Allow(ctx, channel, recipient)
			require.NoError(t, err)
			assert.True(t, allowed, "Request %d should be allowed", i+1)
		}

		// 2. The 101st request MUST be blocked
		allowed, err := limiter.Allow(ctx, channel, recipient)
		require.NoError(t, err)
		assert.False(t, allowed, "The 101st request MUST be rate-limited")

		// 3. Wait for 1 second to allow the bucket to refill
		time.Sleep(1100 * time.Millisecond)

		// 4. Request should now be permitted again
		allowed, err = limiter.Allow(ctx, channel, recipient)
		require.NoError(t, err)
		assert.True(t, allowed, "Request should be allowed after bucket refill")
	})
}

func TestRateLimiter_TokenBucket_Concurrency(t *testing.T) {
	client, teardown := setupTestRedis(t)
	defer teardown()

	limiter := myredis.NewRateLimiter(client)
	ctx := context.Background()
	channel := domain.ChannelEmail

	t.Run("prevents race conditions under extreme concurrency", func(t *testing.T) {
		recipient := "concurrent-user@example.com"
		var wg sync.WaitGroup
		var allowedCount int32
		var blockedCount int32

		// Fire 150 Goroutines simultaneously
		// Since capacity is 100, exactly 100 should pass, and exactly 50 should fail.
		workers := 150
		wg.Add(workers)

		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				allowed, err := limiter.Allow(ctx, channel, recipient)
				require.NoError(t, err)

				if allowed {
					atomic.AddInt32(&allowedCount, 1)
				} else {
					atomic.AddInt32(&blockedCount, 1)
				}
			}()
		}

		wg.Wait()

		// If Lua wasn't atomic, allowedCount would be > 100 because multiple
		// Goroutines would read the same token count before decrementing.
		assert.Equal(t, int32(100), allowedCount, "Exactly 100 requests should be allowed")
		assert.Equal(t, int32(50), blockedCount, "Exactly 50 requests should be blocked")
	})
}
