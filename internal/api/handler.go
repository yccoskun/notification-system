package api

import (
	"context"
	"fmt"
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

// --- DTOs (Data Transfer Objects) ---

type CreateRequest struct {
	IdempotencyKey string             `json:"idempotency_key" binding:"required"`
	Recipient      string             `json:"recipient" binding:"required"`
	Channel        domain.ChannelType `json:"channel" binding:"required,oneof=SMS EMAIL PUSH"`
	TemplateID     *uuid.UUID         `json:"template_id"`
	Priority       int                `json:"priority" binding:"min=1,max=10"`
	Payload        map[string]any     `json:"payload" binding:"required"`
}

type BatchSubmitRequest struct {
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
	Notifications  []struct {
		Recipient  string             `json:"recipient" binding:"required"`
		Channel    domain.ChannelType `json:"channel" binding:"required,oneof=SMS EMAIL PUSH"`
		TemplateID *uuid.UUID         `json:"template_id"`
		Priority   int                `json:"priority" binding:"min=1,max=10"`
		Payload    map[string]any     `json:"payload" binding:"required"`
	} `json:"notifications" binding:"required,max=1000"`
}

// --- Route Handlers ---

// HandleCreate processes a single notification request.
func (h *NotificationHandler) HandleCreate(c *gin.Context) {
	ctx := c.Request.Context()

	var req CreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload schema", "details": err.Error()})
		return
	}

	id := uuid.New()
	now := time.Now()

	notification := &domain.Notification{
		ID:             id,
		BatchID:        nil,
		Recipient:      req.Recipient,
		Channel:        req.Channel,
		TemplateID:     req.TemplateID,
		Priority:       req.Priority,
		Status:         domain.StatusPending,
		Payload:        req.Payload,
		IdempotencyKey: &req.IdempotencyKey,
		SendAt:         now,
	}

	telemetry.NotificationsReceived.WithLabelValues(string(notification.Channel), "api").Inc()

	// Dual-Write: Persist to Postgres first
	if err := h.repo.CreateBatch(ctx, []*domain.Notification{notification}); err != nil {
		slog.ErrorContext(ctx, "failed to insert single notification", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	// Fast-Path: Publish to RabbitMQ
	err := h.publisher.Publish(ctx, notification.ID, notification.Priority)
	if err != nil {
		slog.WarnContext(ctx, "fast-path publish failed, falling back to sweeper", "id", notification.ID, "error", err)
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message":         "notification accepted for processing",
		"notification_id": id.String(),
	})
}

// HandleBatchSubmit processes high-throughput bulk ingress traffic.
func (h *NotificationHandler) HandleBatchSubmit(c *gin.Context) {
	ctx := c.Request.Context()

	var req BatchSubmitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload schema", "details": err.Error()})
		return
	}

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
			TemplateID:     item.TemplateID,
			Priority:       item.Priority,
			Status:         domain.StatusPending,
			Payload:        item.Payload,
			IdempotencyKey: &idempKey,
			SendAt:         now,
		}

		telemetry.NotificationsReceived.WithLabelValues(string(item.Channel), "api").Inc()
	}

	// Dual-Write: Persist to Postgres
	if err := h.repo.CreateBatch(ctx, domainNotifications); err != nil {
		slog.ErrorContext(ctx, "failed to insert notification batch", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	// Fast-Path: Publish to RabbitMQ
	for _, n := range domainNotifications {
		err := h.publisher.Publish(ctx, n.ID, n.Priority)
		if err != nil {
			slog.WarnContext(ctx, "fast-path publish failed, falling back to sweeper", "id", n.ID, "error", err)
		}
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message":  "batch accepted for processing",
		"batch_id": batchID.String(),
		"count":    len(domainNotifications),
	})
}

// HandleGetStatus retrieves the current state of a specific notification.
func (h *NotificationHandler) HandleGetStatus(c *gin.Context) {
	idParam := c.Param("id")
	id, err := uuid.Parse(idParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid notification ID format"})
		return
	}

	notification, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "notification not found"})
		return
	}

	c.JSON(http.StatusOK, notification)
}

// HandleCancel attempts to abort a notification before it is sent.
func (h *NotificationHandler) HandleCancel(c *gin.Context) {
	idParam := c.Param("id")
	id, err := uuid.Parse(idParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid notification ID format"})
		return
	}

	ctx := c.Request.Context()

	notification, err := h.repo.GetByID(ctx, id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "notification not found"})
		return
	}

	if notification.Status != domain.StatusPending {
		c.JSON(http.StatusConflict, gin.H{
			"error":   "cannot cancel notification",
			"details": fmt.Sprintf("notification is currently in status: %s", notification.Status),
		})
		return
	}

	cancelReason := "cancelled by user via API"
	err = h.repo.UpdateStatus(ctx, id, domain.StatusFailed, notification.RetryCount, &cancelReason)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to process cancellation"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "notification successfully cancelled"})
}
