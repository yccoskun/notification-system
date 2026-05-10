package domain

import (
	"time"

	"github.com/google/uuid"
)

type NotificationStatus string

const (
	StatusPending    NotificationStatus = "PENDING"
	StatusProcessing NotificationStatus = "PROCESSING"
	StatusSent       NotificationStatus = "SENT"
	StatusFailed     NotificationStatus = "FAILED"
	StatusCancelled  NotificationStatus = "CANCELLED"
)

type ChannelType string

const (
	ChannelSMS   ChannelType = "SMS"
	ChannelEmail ChannelType = "EMAIL"
	ChannelPush  ChannelType = "PUSH"
)

type Notification struct {
	ID             uuid.UUID
	BatchID        *uuid.UUID
	Recipient      string
	Channel        ChannelType
	TemplateID     *uuid.UUID
	Payload        map[string]any
	Priority       int
	Status         NotificationStatus
	IdempotencyKey *string
	RetryCount     int
	LastError      *string
	SendAt         time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (n *Notification) IsReadyToSend() bool {
	return n.Status == StatusPending && !n.SendAt.After(time.Now())
}
