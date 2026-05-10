package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"notification-system/internal/domain"
	"notification-system/internal/platform/telemetry"
)

// BrokerPublisher is the egress interface to RabbitMQ.
type BrokerPublisher interface {
	Publish(ctx context.Context, notificationID uuid.UUID, priority int) error
}

type NotificationHandler struct {
	repo      domain.NotificationRepository
	publisher BrokerPublisher
}

func NewNotificationHandler(repo domain.NotificationRepository, publisher BrokerPublisher) *NotificationHandler {
	return &NotificationHandler{repo: repo, publisher: publisher}
}

// BatchSubmitRequest defines the expected JSON payload from clients.
type BatchSubmitRequest struct {
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
	Notifications  []struct {
		Recipient string             `json:"recipient" binding:"required"`
		Channel   domain.ChannelType `json:"channel" binding:"required,oneof=SMS EMAIL PUSH"`
		Priority  int                `json:"priority" binding:"min=1,max=10"`
		Payload   map[string]any     `json:"payload" binding:"required"`
	} `json:"notifications" binding:"required,max=1000"`
}

// HandleBatchSubmit processes high-throughput ingress traffic.
func (h *NotificationHandler) HandleBatchSubmit(c *gin.Context) {
	// 1. Extract the trace-injected context from Gin
	ctx := c.Request.Context()

	var req BatchSubmitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload schema", "details": err.Error()})
		return
	}

	// 2. Map HTTP DTOs to Domain Entities
	batchID := uuid.New()
	domainNotifications := make([]*domain.Notification, len(req.Notifications))
	now := time.Now()

	for i, item := range req.Notifications {
		idempKey := req.IdempotencyKey + "-" + item.Recipient // Unique per recipient in batch
		domainNotifications[i] = &domain.Notification{
			ID:             uuid.New(),
			BatchID:        &batchID,
			Recipient:      item.Recipient,
			Channel:        item.Channel,
			Priority:       item.Priority,
			Status:         domain.StatusPending,
			Payload:        item.Payload,
			IdempotencyKey: &idempKey,
			SendAt:         now,
		}

		// Prometheus Metric: Record ingress intent
		telemetry.NotificationsReceived.WithLabelValues(string(item.Channel), "api").Inc()
	}

	// 3. Persist to Database (The Source of Truth)
	if err := h.repo.CreateBatch(ctx, domainNotifications); err != nil {
		slog.ErrorContext(ctx, "failed to insert notification batch", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	// 4. Fast-Path Publish to RabbitMQ
	for _, n := range domainNotifications {
		err := h.publisher.Publish(ctx, n.ID, n.Priority)
		if err != nil {
			// WE DO NOT FAIL THE HTTP REQUEST HERE.
			// We log it. The Sweeper will catch it later.
			slog.WarnContext(ctx, "fast-path publish failed, falling back to sweeper", "id", n.ID, "error", err)
		}
	}

	// 5. Return 202 Accepted (Asynchronous Processing)
	c.JSON(http.StatusAccepted, gin.H{
		"message":  "batch accepted for processing",
		"batch_id": batchID.String(),
		"count":    len(domainNotifications),
	})
}
