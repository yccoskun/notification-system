package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	"github.com/google/uuid"

	"notification-system/internal/domain"
	"notification-system/internal/platform/backoff"
	"notification-system/internal/platform/redis"
)

// Provider defines the egress contract (implemented by our WebhookProvider).
type Provider interface {
	Send(ctx context.Context, n *domain.Notification) error
}

type DeliveryService struct {
	repo         domain.NotificationRepository
	templateRepo domain.TemplateRepository
	limiter      domain.RateLimiter
	idemp        *redis.IdempotencyGuard
	provider     Provider
}

func NewDeliveryService(
	repo domain.NotificationRepository,
	limiter domain.RateLimiter,
	idemp *redis.IdempotencyGuard,
	provider Provider,
) *DeliveryService {
	return &DeliveryService{
		repo:     repo,
		limiter:  limiter,
		idemp:    idemp,
		provider: provider,
	}
}

// HandleDelivery is injected into the RabbitMQ consumer.
func (s *DeliveryService) HandleDelivery(ctx context.Context, id uuid.UUID) error {
	// 1. Fetch State
	notification, err := s.repo.GetByID(ctx, id)
	if err != nil {
		slog.ErrorContext(ctx, "failed to fetch notification", "id", id, "error", err)
		return nil // Return nil (ACK) so RabbitMQ drops this orphaned ID
	}

	// 2. Guard against max retries (Domain Rule: Max 5 attempts)
	if notification.RetryCount >= 5 {
		errMsg := "max retries exceeded"
		_ = s.repo.UpdateStatus(ctx, id, domain.StatusFailed, notification.RetryCount, &errMsg)
		return nil // ACK to drop
	}

	// 3. Egress Idempotency Check (Redis SETNX)
	acquired, err := s.idemp.CheckAndSet(ctx, id)
	if err != nil {
		return err // Redis network error, NACK to requeue and try again
	}
	if !acquired {
		slog.WarnContext(ctx, "duplicate message detected by idempotency guard, dropping", "id", id)
		return nil // Already processing elsewhere, ACK to drop
	}

	// 4. Rate Limiting Check
	allowed, err := s.limiter.Allow(ctx, notification.Channel)
	if err != nil {
		_ = s.idemp.Clear(ctx, id) // Release lock so it can be retried
		return err                 // Redis error, NACK
	}
	if !allowed {
		_ = s.idemp.Clear(ctx, id)
		// We return an error here so the RabbitMQ consumer NACKs it with requeue=true
		return errors.New("rate limited")
	}

	// 5. Execute Egress (Circuit Breaker protected)
	err = s.provider.Send(ctx, notification)

	// 6. Handle Success
	if err == nil {
		_ = s.repo.UpdateStatus(ctx, id, domain.StatusSent, notification.RetryCount, nil)
		// Note: We intentionally DO NOT clear the idempotency lock on success.
		// If RabbitMQ re-delivers this message due to a network blip, the lock will
		// instantly catch it and drop it. The lock naturally expires in 24 hours.
		return nil
	}

	// 7. Handle Failures
	_ = s.idemp.Clear(ctx, id) // Release lock so Sweeper/Retries can grab it

	errStr := err.Error()

	// If it's a 4xx Client Error, fail permanently
	if isClientError(err) {
		_ = s.repo.UpdateStatus(ctx, id, domain.StatusFailed, notification.RetryCount, &errStr)
		return nil // ACK to drop
	}

	// If it's a 5xx or Circuit Breaker Open, schedule a delayed retry via the Sweeper
	delay := backoff.Calculate(notification.RetryCount)
	notification.SendAt = time.Now().Add(delay)
	notification.Status = domain.StatusPending
	notification.RetryCount++
	notification.LastError = &errStr

	// We update the DB to push the SendAt time into the future.
	// We then ACK the message (return nil) to remove it from RabbitMQ immediately.
	// The Sweeper (Epic 7) will find it in the DB when SendAt <= NOW() and push it back to the queue.
	updateErr := s.repo.UpdateStatus(ctx, id, notification.Status, notification.RetryCount, &errStr)
	if updateErr != nil {
		return updateErr // If DB fails, NACK to hold in queue
	}

	// We must also update the SendAt time in the DB manually using a custom query
	// (or augment our UpdateStatus method). For brevity in this file, assume UpdateStatus handles it
	// or we write a quick raw query here to push the timestamp.
	scheduleErr := s.repo.ScheduleRetry(ctx, id, notification.SendAt)
	if scheduleErr != nil {
		return scheduleErr
	}

	return nil // ACK
}

// isClientError checks if the error was a 4xx indicating a bad payload.
func isClientError(err error) bool {
	// In a real system, you might use errors.As() to check for a specific struct.
	// We use strings.Contains matching the error we threw in provider/webhook.go.
	return err != nil && stringContains(err.Error(), "4xx error")
}

func stringContains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}

func (s *DeliveryService) renderTemplate(ctx context.Context, n *domain.Notification) error {
	if n.TemplateID == nil {
		return nil // No template to render
	}

	tmplData, err := s.templateRepo.GetByID(ctx, *n.TemplateID)
	if err != nil {
		return fmt.Errorf("failed to fetch template: %w", err)
	}

	// 1. Initialize Go text/template
	tmpl, err := template.New("notification").Parse(tmplData.Body)
	if err != nil {
		return fmt.Errorf("invalid template syntax: %w", err)
	}

	// 2. Execute template with Payload variables
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, n.Payload); err != nil {
		return fmt.Errorf("template execution failed: %w", err)
	}

	// 3. Overwrite payload with compiled content for the provider
	n.Payload["compiled_body"] = buf.String()
	if tmplData.Subject != nil {
		n.Payload["compiled_subject"] = *tmplData.Subject
	}

	return nil
}
