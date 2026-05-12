package service

import (
	"bytes"
	"context"
	"fmt"
	"text/template"
	"time"

	"notification-system/internal/domain"
	"notification-system/internal/platform/backoff"
	"notification-system/internal/platform/telemetry"

	"github.com/google/uuid"
)

// StatusPublisher is defined locally for the service layer
type StatusPublisher interface {
	Publish(ctx context.Context, id, status string) error
}

type Provider interface {
	Send(ctx context.Context, n *domain.Notification) error
}

type DeliveryService struct {
	repo         domain.NotificationRepository
	templateRepo domain.TemplateRepository
	limiter      domain.RateLimiter
	idemp        domain.IdempotencyGuard
	providers    map[domain.ChannelType]Provider
	statusPub    StatusPublisher
}

func (s *DeliveryService) HandleDelivery(ctx context.Context, id uuid.UUID) error {
	n, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to fetch notification %s: %w", id, err)
	}

	if n.Status != domain.StatusPending {
		return nil
	}

	// 1. Idempotency
	locked, err := s.idemp.Acquire(ctx, id.String(), 10*time.Minute)
	if err != nil || !locked {
		telemetry.L(ctx).ErrorContext(ctx, "could not acquire idempotency lock, redis down: moving notification to hibernation", "error", err)
		_ = s.repo.ScheduleRetry(ctx, id, time.Now().Add(5*time.Minute))
		return nil
	}
	defer func() {
		if err := s.idemp.Release(ctx, id.String()); err != nil {
			telemetry.L(ctx).WarnContext(ctx, "failed to release idempotency lock", "error", err)
		}
	}()

	p, ok := s.providers[n.Channel]
	if !ok {
		return fmt.Errorf("no provider configured for channel: %s", n.Channel)
	}

	// 2. Rate Limiting
	allowed, err := s.limiter.Allow(ctx, "ratelimit:"+string(n.Channel)+":"+n.Recipient)
	if err != nil {
		telemetry.L(ctx).ErrorContext(ctx, "rate limiter unreachable, backing off", "error", err)
		return s.repo.ScheduleRetry(ctx, id, time.Now().Add(1*time.Minute))
	}

	if !allowed {
		telemetry.L(ctx).InfoContext(ctx, "rate limit hit: delaying notification")
		telemetry.RateLimitHits.WithLabelValues(string(n.Channel)).Inc()
		return s.repo.ScheduleRetry(ctx, id, time.Now().Add(1*time.Minute))
	}

	// 3. Rendering
	if n.TemplateID != nil {
		if err := s.renderTemplate(ctx, n); err != nil {
			return s.handleFailure(ctx, n, err, false)
		}
	}

	// 4. Provider Call
	// DeliveryLatency is observed inside WebhookProvider.Send (the authoritative place),
	// so we do not record it again here to avoid double-counting.
	err = p.Send(ctx, n)

	if err != nil {
		return s.handleFailure(ctx, n, err, true)
	}

	// 5. Success
	telemetry.NotificationsSent.WithLabelValues(string(n.Channel), "success").Inc()
	if err := s.repo.UpdateStatus(ctx, id, domain.StatusSent, n.RetryCount, nil); err == nil {
		_ = s.statusPub.Publish(ctx, id.String(), string(domain.StatusSent))
	}

	return nil
}

func (s *DeliveryService) renderTemplate(ctx context.Context, n *domain.Notification) error {
	tmplData, err := s.templateRepo.GetByID(ctx, *n.TemplateID)
	if err != nil {
		return err
	}

	tmpl, err := template.New("notif").Parse(tmplData.Body)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, n.Payload); err != nil {
		return err
	}

	// Standardize these keys so all providers know where to look
	n.Payload["_rendered_content"] = buf.String()
	if tmplData.Subject != nil {
		n.Payload["_rendered_subject"] = *tmplData.Subject
	}
	return nil
}

func (s *DeliveryService) handleFailure(ctx context.Context, n *domain.Notification, err error, retryable bool) error {
	errMsg := err.Error()
	telemetry.NotificationsSent.WithLabelValues(string(n.Channel), "error").Inc()

	if !retryable || n.RetryCount >= 5 {
		_ = s.repo.UpdateStatus(ctx, n.ID, domain.StatusFailed, n.RetryCount, &errMsg)
		_ = s.statusPub.Publish(ctx, n.ID.String(), string(domain.StatusFailed))
		return nil
	}

	retryAt := time.Now().Add(backoff.Calculate(n.RetryCount))
	_ = s.repo.UpdateStatus(ctx, n.ID, domain.StatusPending, n.RetryCount+1, &errMsg)
	return s.repo.ScheduleRetry(ctx, n.ID, retryAt)
}

func NewDeliveryService(
	repo domain.NotificationRepository,
	tmplRepo domain.TemplateRepository,
	limiter domain.RateLimiter,
	idemp domain.IdempotencyGuard,
	providers map[domain.ChannelType]Provider, // Updated arg
	statusPub StatusPublisher,
) *DeliveryService {
	return &DeliveryService{
		repo:         repo,
		templateRepo: tmplRepo,
		limiter:      limiter,
		idemp:        idemp,
		providers:    providers,
		statusPub:    statusPub,
	}
}
