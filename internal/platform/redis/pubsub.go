package redis

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

const StatusChannel = "notification_status_updates"

// StatusUpdate is the JSON payload broadcast to WebSocket clients.
type StatusUpdate struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	// Detail is optional machine-readable context (e.g. rate_limited, retry_scheduled).
	Detail string `json:"detail,omitempty"`
}

type PubSub struct {
	client *redis.Client
}

func NewPubSub(client *redis.Client) *PubSub {
	return &PubSub{client: client}
}

// Publish broadcasts a terminal or lifecycle status without extra detail.
func (p *PubSub) Publish(ctx context.Context, id, status string) error {
	return p.PublishWithDetail(ctx, id, status, "")
}

// PublishWithDetail broadcasts a status update; empty detail is omitted from JSON.
func (p *PubSub) PublishWithDetail(ctx context.Context, id, status, detail string) error {
	msg, err := json.Marshal(StatusUpdate{ID: id, Status: status, Detail: detail})
	if err != nil {
		return err
	}
	return p.client.Publish(ctx, StatusChannel, msg).Err()
}

// Subscribe (Used by the API) listens for status changes.
func (p *PubSub) Subscribe(ctx context.Context) <-chan *redis.Message {
	pubsub := p.client.Subscribe(ctx, StatusChannel)
	return pubsub.Channel()
}
