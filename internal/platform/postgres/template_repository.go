package postgres

import (
	"context"
	"notification-system/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TemplateRepository struct {
	db *pgxpool.Pool
}

func NewTemplateRepository(db *pgxpool.Pool) *TemplateRepository {
	return &TemplateRepository{db: db}
}

func (r *TemplateRepository) Create(ctx context.Context, t *domain.Template) error {
	query := `INSERT INTO templates (name, channel, subject, body) VALUES ($1, $2, $3, $4) RETURNING id, created_at, updated_at`
	return r.db.QueryRow(ctx, query, t.Name, t.Channel, t.Subject, t.Body).Scan(&t.ID, &t.CreatedAt, &t.UpdatedAt)
}

func (r *TemplateRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	query := `SELECT id, name, channel, subject, body, created_at, updated_at FROM templates WHERE id = $1`
	var t domain.Template
	err := r.db.QueryRow(ctx, query, id).Scan(&t.ID, &t.Name, &t.Channel, &t.Subject, &t.Body, &t.CreatedAt, &t.UpdatedAt)
	return &t, err
}

func (r *TemplateRepository) GetByName(ctx context.Context, name string) (*domain.Template, error) {
	query := `SELECT id, name, channel, subject, body, created_at, updated_at FROM templates WHERE name = $1`
	var t domain.Template
	err := r.db.QueryRow(ctx, query, name).Scan(&t.ID, &t.Name, &t.Channel, &t.Subject, &t.Body, &t.CreatedAt, &t.UpdatedAt)
	return &t, err
}
