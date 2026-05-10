package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Template struct {
	ID        uuid.UUID   `json:"id"`
	Name      string      `json:"name"`
	Channel   ChannelType `json:"channel"`
	Subject   *string     `json:"subject,omitempty"`
	Body      string      `json:"body"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

type TemplateRepository interface {
	Create(ctx context.Context, t *Template) error
	GetByID(ctx context.Context, id uuid.UUID) (*Template, error)
	GetByName(ctx context.Context, name string) (*Template, error)
	Delete(ctx context.Context, id uuid.UUID) error
}
