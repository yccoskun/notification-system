package api_test

import (
	"context"
)

// noopStatusPublisher satisfies api.StatusBroadcaster for handler tests.
type noopStatusPublisher struct{}

func (noopStatusPublisher) Publish(context.Context, string, string) error { return nil }

func (noopStatusPublisher) PublishWithDetail(context.Context, string, string, string) error {
	return nil
}
