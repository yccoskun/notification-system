package api

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"notification-system/internal/domain"

	"github.com/stretchr/testify/require"
)

func TestValidateCreateRequest_SMS_nonTemplate(t *testing.T) {
	req := &CreateRequest{
		IdempotencyKey: "k1",
		Recipient:      "+15551234567",
		Channel:        domain.ChannelSMS,
		Payload:        map[string]any{"message": "hello"},
	}
	require.Empty(t, ValidateCreateRequest(req))

	req2 := &CreateRequest{
		IdempotencyKey: "k1",
		Recipient:      "+15551234567",
		Channel:        domain.ChannelSMS,
		Payload:        map[string]any{"body": "alias"},
	}
	require.Empty(t, ValidateCreateRequest(req2))

	req3 := &CreateRequest{
		IdempotencyKey: "k1",
		Recipient:      "+15551234567",
		Channel:        domain.ChannelSMS,
		Payload:        map[string]any{},
	}
	errs := ValidateCreateRequest(req3)
	require.NotEmpty(t, errs)
	require.Equal(t, "payload.message", errs[0].Path)
}

func TestValidateCreateRequest_Email_nonTemplate(t *testing.T) {
	req := &CreateRequest{
		IdempotencyKey: "k1",
		Recipient:      "a@b.co",
		Channel:        domain.ChannelEmail,
		Payload: map[string]any{
			"subject": "Hi",
			"message": "Body",
		},
	}
	require.Empty(t, ValidateCreateRequest(req))
}

func TestValidateCreateRequest_Push_nonTemplate(t *testing.T) {
	req := &CreateRequest{
		IdempotencyKey: "k1",
		Recipient:      "device-token",
		Channel:        domain.ChannelPush,
		Payload: map[string]any{
			"title":   "T",
			"message": "M",
		},
	}
	require.Empty(t, ValidateCreateRequest(req))
}

func TestValidateCreateRequest_reservedKeys(t *testing.T) {
	tid := uuid.New()
	req := &CreateRequest{
		IdempotencyKey: "k1",
		Recipient:      "r",
		Channel:        domain.ChannelSMS,
		TemplateID:     &tid,
		Payload:        map[string]any{"name": "x", "_rendered_content": "inject"},
	}
	errs := ValidateCreateRequest(req)
	require.NotEmpty(t, errs)
	var found bool
	for _, e := range errs {
		if strings.Contains(e.Path, "_rendered_content") {
			found = true
		}
	}
	require.True(t, found)
}

func TestValidateCreateRequest_template_emptyPayload(t *testing.T) {
	tid := uuid.New()
	req := &CreateRequest{
		IdempotencyKey: "k1",
		Recipient:      "r",
		Channel:        domain.ChannelSMS,
		TemplateID:     &tid,
		Payload:        nil,
	}
	require.Empty(t, ValidateCreateRequest(req))
	require.NotNil(t, req.Payload)
	require.Empty(t, req.Payload)
}

func TestValidateCreateRequest_idempotencyTrims(t *testing.T) {
	req := &CreateRequest{
		IdempotencyKey: "  abc  ",
		Recipient:      "  +1  ",
		Channel:        domain.ChannelSMS,
		Payload:        map[string]any{"message": "m"},
	}
	require.Empty(t, ValidateCreateRequest(req))
	require.Equal(t, "abc", req.IdempotencyKey)
	require.Equal(t, "+1", req.Recipient)
}

func TestValidateBatchSubmitRequest_compositeIdempotencyLength(t *testing.T) {
	longKey := strings.Repeat("a", 200)
	longRec := strings.Repeat("b", 60) // 200+1+60 > 255
	req := &BatchSubmitRequest{
		IdempotencyKey: longKey,
		Notifications: []BatchNotificationItem{{
			Recipient: longRec,
			Channel:   domain.ChannelSMS,
			Payload:   map[string]any{"message": "x"},
		}},
	}
	errs := ValidateBatchSubmitRequest(req)
	require.NotEmpty(t, errs)
}

func TestValidateBatchSubmitRequest_multipleRows(t *testing.T) {
	req := &BatchSubmitRequest{
		IdempotencyKey: "batch-1",
		Notifications: []BatchNotificationItem{
			{Recipient: "a", Channel: domain.ChannelSMS, Payload: map[string]any{"message": "1"}},
			{Recipient: "b", Channel: domain.ChannelSMS, Payload: map[string]any{}},
		},
	}
	errs := ValidateBatchSubmitRequest(req)
	require.Len(t, errs, 1)
	require.Equal(t, "notifications[1].payload.message", errs[0].Path)
}

func TestNormalizedPriority(t *testing.T) {
	require.Equal(t, 5, normalizedPriority(0))
	require.Equal(t, 3, normalizedPriority(3))
}
