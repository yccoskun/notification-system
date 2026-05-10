package domain

import (
	"time"

	"github.com/google/uuid"
)

type Template struct {
	ID        uuid.UUID
	Name      string
	Content   string
	CreatedAt time.Time
	UpdatedAt time.Time
}
