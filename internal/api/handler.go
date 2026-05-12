package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
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
	IdempotencyKey string             `json:"idempotency_key" binding:"required,max=255"`
	Recipient      string             `json:"recipient" binding:"required,max=255"`
	Channel        domain.ChannelType `json:"channel" binding:"required,oneof=SMS EMAIL PUSH"`
	TemplateID     *uuid.UUID         `json:"template_id"`
	Priority       int                `json:"priority" binding:"gte=0,lte=10"`
	Payload        map[string]any     `json:"payload"`
}

// BatchNotificationItem is one row in a batch submit request.
type BatchNotificationItem struct {
	Recipient  string             `json:"recipient" binding:"required,max=255"`
	Channel    domain.ChannelType `json:"channel" binding:"required,oneof=SMS EMAIL PUSH"`
	TemplateID *uuid.UUID         `json:"template_id"`
	Priority   int                `json:"priority" binding:"gte=0,lte=10"`
	Payload    map[string]any     `json:"payload"`
}

type BatchSubmitRequest struct {
	IdempotencyKey string                  `json:"idempotency_key" binding:"required,max=255"`
	Notifications  []BatchNotificationItem `json:"notifications" binding:"required,min=1,max=1000,dive"`
}

// --- Route Handlers ---

const (
	listDefaultLimit = 50
	listMaxLimit     = 100
)

// HandleList returns notifications with optional filters (status, channel, created_at range) and offset pagination.
func (h *NotificationHandler) HandleList(c *gin.Context) {
	ctx := c.Request.Context()

	var filter domain.NotificationListFilter

	if s := c.Query("status"); s != "" {
		st := domain.NotificationStatus(s)
		switch st {
		case domain.StatusPending, domain.StatusSent, domain.StatusFailed, domain.StatusCancelled:
			filter.Status = &st
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status", "details": "must be one of PENDING, SENT, FAILED, CANCELLED"})
			return
		}
	}

	if ch := c.Query("channel"); ch != "" {
		ct := domain.ChannelType(ch)
		switch ct {
		case domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush:
			filter.Channel = &ct
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid channel", "details": "must be one of SMS, EMAIL, PUSH"})
			return
		}
	}

	parseTimeParam := func(name, raw string) (time.Time, error) {
		if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			return t, nil
		}
		return time.Parse(time.RFC3339, raw)
	}

	if raw := c.Query("created_from"); raw != "" {
		t, err := parseTimeParam("created_from", raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid created_from", "details": "use RFC3339 or RFC3339Nano"})
			return
		}
		filter.CreatedFrom = &t
	}
	if raw := c.Query("created_to"); raw != "" {
		t, err := parseTimeParam("created_to", raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid created_to", "details": "use RFC3339 or RFC3339Nano"})
			return
		}
		filter.CreatedTo = &t
	}

	if filter.CreatedFrom != nil && filter.CreatedTo != nil && filter.CreatedFrom.After(*filter.CreatedTo) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid date range", "details": "created_from must be before or equal to created_to"})
		return
	}

	limit := listDefaultLimit
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit", "details": "must be a positive integer"})
			return
		}
		if n > listMaxLimit {
			n = listMaxLimit
		}
		limit = n
	}

	offset := 0
	if raw := c.Query("offset"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid offset", "details": "must be a non-negative integer"})
			return
		}
		offset = n
	}

	items, total, err := h.repo.List(ctx, filter, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"notifications": items,
		"total":         total,
		"limit":         limit,
		"offset":        offset,
	})
}

// HandleCreate processes a single notification request.
func (h *NotificationHandler) HandleCreate(c *gin.Context) {
	ctx := c.Request.Context()

	var req CreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload schema", "details": err.Error()})
		return
	}

	if fieldErrs := ValidateCreateRequest(&req); len(fieldErrs) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "validation failed", "fields": fieldErrs})
		return
	}

	id := uuid.New()
	now := time.Now()
	priority := normalizedPriority(req.Priority)
	notification := &domain.Notification{
		ID:             id,
		BatchID:        nil,
		Recipient:      req.Recipient,
		Channel:        req.Channel,
		TemplateID:     req.TemplateID,
		Priority:       priority,
		Status:         domain.StatusPending,
		Payload:        req.Payload,
		IdempotencyKey: &req.IdempotencyKey,
		SendAt:         now,
	}

	telemetry.NotificationsReceived.WithLabelValues(string(notification.Channel), "api").Inc()

	// Dual-Write: Persist to Postgres first
	insertedIDs, err := h.repo.CreateBatch(ctx, []*domain.Notification{notification})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	actualID := insertedIDs[0]

	// If the ID from the DB is the same as the one we just made, it's NEW.
	// If it's different, it's a DUPLICATE.
	if actualID == notification.ID {
		err := h.publisher.Publish(ctx, actualID, notification.Priority)
		if err != nil {
			telemetry.L(ctx).WarnContext(ctx, "fast-path publish failed; notification remains PENDING for sweeper", "id", notification.ID, "error", err)
		}
	} else {
		telemetry.L(ctx).InfoContext(ctx, "ignoring duplicate ingress request", "key", req.IdempotencyKey)
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message":         "notification accepted for processing",
		"notification_id": actualID.String(),
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

	if fieldErrs := ValidateBatchSubmitRequest(&req); len(fieldErrs) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "validation failed", "fields": fieldErrs})
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
			Priority:       normalizedPriority(item.Priority),
			Status:         domain.StatusPending,
			Payload:        item.Payload,
			IdempotencyKey: &idempKey,
			SendAt:         now,
		}

		telemetry.NotificationsReceived.WithLabelValues(string(item.Channel), "api").Inc()
	}

	// Dual-Write: Persist to Postgres
	insertedIDs, err := h.repo.CreateBatch(ctx, domainNotifications)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	// Create a map for fast lookup of what was actually inserted
	insertedMap := make(map[uuid.UUID]bool)
	for _, id := range insertedIDs {
		insertedMap[id] = true
	}

	// Fast-Path: Publish to RabbitMQ
	for _, n := range domainNotifications {
		if insertedMap[n.ID] {
			err := h.publisher.Publish(ctx, n.ID, n.Priority)
			if err != nil {
				telemetry.L(ctx).WarnContext(ctx, "fast-path publish failed; notification remains PENDING for sweeper", "id", n.ID, "error", err)
			}
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

// HandleGetBatchStatus returns the current state of every notification in an ingress batch.
func (h *NotificationHandler) HandleGetBatchStatus(c *gin.Context) {
	batchParam := c.Param("batch_id")
	batchID, err := uuid.Parse(batchParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid batch ID format"})
		return
	}

	notifications, err := h.repo.GetByBatchID(c.Request.Context(), batchID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"batch_id":      batchID.String(),
		"count":         len(notifications),
		"notifications": notifications,
	})
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
	err = h.repo.UpdateStatus(ctx, id, domain.StatusCancelled, notification.RetryCount, &cancelReason)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to process cancellation"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "notification successfully cancelled"})
}
