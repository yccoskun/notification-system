package redis

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

const StatusChannel = "notification_status_updates"

// StatusUpdate is the payload we broadcast to clients.
type StatusUpdate struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type PubSub struct {
	client *redis.Client
}

func NewPubSub(client *redis.Client) *PubSub {
	return &PubSub{client: client}
}

// Publish (Used by the Worker) broadcasts the status change.
func (p *PubSub) Publish(ctx context.Context, id, status string) error {
	msg, _ := json.Marshal(StatusUpdate{ID: id, Status: status})
	return p.client.Publish(ctx, StatusChannel, msg).Err()
}

// Subscribe (Used by the API) listens for status changes.
func (p *PubSub) Subscribe(ctx context.Context) <-chan *redis.Message {
	pubsub := p.client.Subscribe(ctx, StatusChannel)
	return pubsub.Channel()
}
