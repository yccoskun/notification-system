package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"notification-system/internal/domain"
)

// tokenBucketLua is evaluated atomically by Redis.
const tokenBucketLua = `
local rate_limit_key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate_per_sec = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = 1

local state = redis.call("HMGET", rate_limit_key, "tokens", "last_refill")
local tokens = tonumber(state[1]) or capacity
local last_refill = tonumber(state[2]) or now

local elapsed_time = math.max(0, now - last_refill)
local refill_amount = math.floor(elapsed_time * refill_rate_per_sec)

tokens = math.min(capacity, tokens + refill_amount)

if refill_amount > 0 then
    last_refill = now
end

local allowed = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
end

redis.call("HMSET", rate_limit_key, "tokens", tokens, "last_refill", last_refill)
redis.call("EXPIRE", rate_limit_key, 60)

return allowed
`

// RateLimiter implements domain.RateLimiter.
type RateLimiter struct {
	client     *redis.Client
	script     *redis.Script
	capacity   int
	refillRate int
}

// NewRateLimiter initializes the limiter with strict 100 req/sec parameters.
func NewRateLimiter(client *redis.Client) *RateLimiter {
	return &RateLimiter{
		client:     client,
		script:     redis.NewScript(tokenBucketLua),
		capacity:   100, // Maximum burst
		refillRate: 100, // Refill 100 tokens per second
	}
}

// Allow executes the Lua script to safely check and decrement the token bucket.
func (rl *RateLimiter) Allow(ctx context.Context, channel domain.ChannelType, recipient string) (bool, error) {
	key := fmt.Sprintf("ratelimit:%s:%s", channel, recipient)
	now := time.Now().Unix()

	// Run evaluates the cached SHA of the script, saving massive network bandwidth.
	result, err := rl.script.Run(ctx, rl.client, []string{key}, rl.capacity, rl.refillRate, now).Result()
	if err != nil {
		return false, fmt.Errorf("redis rate limit script failed: %w", err)
	}

	return result.(int64) == 1, nil
}
