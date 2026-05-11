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
	provider     Provider
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
		return fmt.Errorf("could not acquire idempotency lock")
	}
	defer s.idemp.Release(ctx, id.String())

	// 2. Rate Limiting (Fixed Signature)
	allowed, err := s.limiter.Allow(ctx, n.Channel, n.Recipient)
	if err != nil || !allowed {
		telemetry.RateLimitHits.WithLabelValues(string(n.Channel)).Inc()
		return s.repo.ScheduleRetry(ctx, id, time.Now().Add(time.Minute))
	}

	// 3. Rendering
	if n.TemplateID != nil {
		if err := s.renderTemplate(ctx, n); err != nil {
			return s.handleFailure(ctx, n, err, false)
		}
	}

	// 4. Provider Call & Latency Tracking
	start := time.Now()
	err = s.provider.Send(ctx, n)
	duration := time.Since(start).Seconds()

	// FIXED: Observe requires label values for a HistogramVec
	telemetry.DeliveryLatency.WithLabelValues(string(n.Channel)).Observe(duration)

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
	templateRepo domain.TemplateRepository,
	limiter domain.RateLimiter,
	idemp domain.IdempotencyGuard,
	provider Provider,
	statusPub StatusPublisher,
) *DeliveryService {
	return &DeliveryService{
		repo:         repo,
		templateRepo: templateRepo,
		limiter:      limiter,
		idemp:        idemp,
		provider:     provider,
		statusPub:    statusPub,
	}
}
