package domain

import "context"

type RateLimiter interface {
	Allow(ctx context.Context, channel ChannelType, recipient string) (bool, error)
}
