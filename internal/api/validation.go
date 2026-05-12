package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"notification-system/internal/domain"
)

// Limits aligned with PostgreSQL schema (VARCHAR(255)) and practical provider caps.
const (
	maxDBVarchar = 255

	// MaxScheduleHorizon caps how far in the future send_at may be set.
	MaxScheduleHorizon = 366 * 24 * time.Hour

	// MaxPayloadJSONBytes caps JSONB size and outbound webhook bodies.
	MaxPayloadJSONBytes = 512 << 10 // 512 KiB

	maxSMSBodyRunes       = 1600
	maxEmailSubjectRunes  = 255
	maxEmailBodyRunes     = 100_000
	maxPushTitleRunes     = 128
	maxPushBodyRunes      = 4096
	maxTemplateStringRunes = 8192
)

var reservedPayloadKeys = map[string]struct{}{
	"_rendered_content": {},
	"_rendered_subject": {},
}

// FieldError is a single validation problem with a stable field path for clients.
type FieldError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

func (e FieldError) Error() string {
	return e.Path + ": " + e.Message
}

func trimField(s *string) {
	*s = strings.TrimSpace(*s)
}

func runeLen(s string) int {
	return utf8.RuneCountInString(s)
}

func validateVarchar(path, s string, required bool) []FieldError {
	if required && s == "" {
		return []FieldError{{Path: path, Message: "required"}}
	}
	if !required && s == "" {
		return nil
	}
	if rl := runeLen(s); rl > maxDBVarchar {
		return []FieldError{{Path: path, Message: fmt.Sprintf("must be at most %d characters", maxDBVarchar)}}
	}
	if !utf8.ValidString(s) {
		return []FieldError{{Path: path, Message: "must be valid UTF-8"}}
	}
	return nil
}

func validatePriority(path string, p int) []FieldError {
	if p < 0 || p > 10 {
		return []FieldError{{Path: path, Message: "must be between 0 and 10 (0 means default priority)"}}
	}
	return nil
}

// validateOptionalSendAt rejects send_at values too far in the future.
func validateOptionalSendAt(path string, sendAt *time.Time) []FieldError {
	if sendAt == nil {
		return nil
	}
	max := time.Now().Add(MaxScheduleHorizon)
	if sendAt.After(max) {
		return []FieldError{{
			Path:    path,
			Message: fmt.Sprintf("must be at most %d days in the future", int(MaxScheduleHorizon/(24*time.Hour))),
		}}
	}
	return nil
}

func payloadJSONByteLen(payload map[string]any) (int, error) {
	if payload == nil {
		return 0, nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func reservedPayloadErrors(pathPrefix string, payload map[string]any) []FieldError {
	if payload == nil {
		return nil
	}
	var out []FieldError
	for k := range payload {
		if _, bad := reservedPayloadKeys[k]; bad {
			out = append(out, FieldError{
				Path:    pathPrefix + "." + k,
				Message: "reserved key; remove this field from the payload",
			})
		}
	}
	return out
}

func stringFromPayload(payload map[string]any, keys ...string) (string, bool, []FieldError) {
	for _, key := range keys {
		raw, ok := payload[key]
		if !ok {
			continue
		}
		s, ok := raw.(string)
		if !ok {
			return "", false, []FieldError{{Path: key, Message: "must be a string"}}
		}
		if strings.TrimSpace(s) != "" {
			return s, true, nil
		}
	}
	return "", false, nil
}

func validateNonTemplateChannelContent(pathPrefix string, ch domain.ChannelType, payload map[string]any) []FieldError {
	var out []FieldError
	p := func(field string) string {
		if pathPrefix == "" {
			return "payload." + field
		}
		return pathPrefix + "." + field
	}

	switch ch {
	case domain.ChannelSMS:
		body, ok, errs := stringFromPayload(payload, "message", "body")
		if len(errs) > 0 {
			for i := range errs {
				errs[i].Path = p(errs[i].Path)
			}
			return errs
		}
		if !ok {
			return []FieldError{{Path: p("message"), Message: "required when template_id is omitted (body is accepted as an alias)"}}
		}
		if rl := runeLen(body); rl > maxSMSBodyRunes {
			return []FieldError{{Path: p("message"), Message: fmt.Sprintf("must be at most %d characters for SMS", maxSMSBodyRunes)}}
		}

	case domain.ChannelEmail:
		rawSub, ok := payload["subject"]
		if !ok {
			return []FieldError{{Path: p("subject"), Message: "required when template_id is omitted"}}
		}
		subject, ok := rawSub.(string)
		if !ok {
			return []FieldError{{Path: p("subject"), Message: "must be a string"}}
		}
		subject = strings.TrimSpace(subject)
		if subject == "" {
			return []FieldError{{Path: p("subject"), Message: "must not be empty"}}
		}
		if rl := runeLen(subject); rl > maxEmailSubjectRunes {
			return []FieldError{{Path: p("subject"), Message: fmt.Sprintf("must be at most %d characters", maxEmailSubjectRunes)}}
		}

		body, ok, errs := stringFromPayload(payload, "message", "body")
		if len(errs) > 0 {
			for i := range errs {
				errs[i].Path = p(errs[i].Path)
			}
			return errs
		}
		if !ok {
			return []FieldError{{Path: p("message"), Message: "required when template_id is omitted (body is accepted as an alias)"}}
		}
		if rl := runeLen(body); rl > maxEmailBodyRunes {
			return []FieldError{{Path: p("message"), Message: fmt.Sprintf("must be at most %d characters", maxEmailBodyRunes)}}
		}

	case domain.ChannelPush:
		rawTitle, ok := payload["title"]
		if !ok {
			return []FieldError{{Path: p("title"), Message: "required when template_id is omitted"}}
		}
		title, ok := rawTitle.(string)
		if !ok {
			return []FieldError{{Path: p("title"), Message: "must be a string"}}
		}
		title = strings.TrimSpace(title)
		if title == "" {
			return []FieldError{{Path: p("title"), Message: "must not be empty"}}
		}
		if rl := runeLen(title); rl > maxPushTitleRunes {
			return []FieldError{{Path: p("title"), Message: fmt.Sprintf("must be at most %d characters", maxPushTitleRunes)}}
		}

		body, ok, errs := stringFromPayload(payload, "message", "body")
		if len(errs) > 0 {
			for i := range errs {
				errs[i].Path = p(errs[i].Path)
			}
			return errs
		}
		if !ok {
			return []FieldError{{Path: p("message"), Message: "required when template_id is omitted (body is accepted as an alias)"}}
		}
		if rl := runeLen(body); rl > maxPushBodyRunes {
			return []FieldError{{Path: p("message"), Message: fmt.Sprintf("must be at most %d characters", maxPushBodyRunes)}}
		}
	}
	return out
}

func validatePayloadStringDepth(path string, v any) []FieldError {
	switch x := v.(type) {
	case string:
		if rl := runeLen(x); rl > maxTemplateStringRunes {
			return []FieldError{{Path: path, Message: fmt.Sprintf("must be at most %d characters", maxTemplateStringRunes)}}
		}
		if !utf8.ValidString(x) {
			return []FieldError{{Path: path, Message: "must be valid UTF-8"}}
		}
		return nil
	case map[string]any:
		var out []FieldError
		for k, vv := range x {
			out = append(out, validatePayloadStringDepth(path+"."+k, vv)...)
		}
		return out
	case []any:
		var out []FieldError
		for i, vv := range x {
			out = append(out, validatePayloadStringDepth(fmt.Sprintf("%s[%d]", path, i), vv)...)
		}
		return out
	default:
		return nil
	}
}

func validateTemplatePayload(pathPrefix string, payload map[string]any) []FieldError {
	if payload == nil {
		return nil
	}
	var out []FieldError
	out = append(out, reservedPayloadErrors(pathPrefix, payload)...)
	for k, v := range payload {
		if _, bad := reservedPayloadKeys[k]; bad {
			continue
		}
		base := pathPrefix
		if base == "" {
			base = "payload"
		}
		out = append(out, validatePayloadStringDepth(base+"."+k, v)...)
	}
	return out
}

func payloadFieldPath(pathPrefix string) string {
	if pathPrefix == "" {
		return "payload"
	}
	return pathPrefix + "payload"
}

func validatePayloadCommon(pathPrefix string, payload map[string]any) []FieldError {
	path := payloadFieldPath(pathPrefix)
	if payload == nil {
		return []FieldError{{Path: path, Message: "required"}}
	}
	n, err := payloadJSONByteLen(payload)
	if err != nil {
		return []FieldError{{Path: path, Message: "must be JSON-serializable"}}
	}
	if n > MaxPayloadJSONBytes {
		return []FieldError{{Path: path, Message: fmt.Sprintf("serialized size must be at most %d bytes", MaxPayloadJSONBytes)}}
	}
	return nil
}

// ValidateCreateRequest checks ingress rules after JSON binding. It may trim
// idempotency_key and recipient in place.
func ValidateCreateRequest(req *CreateRequest) []FieldError {
	trimField(&req.IdempotencyKey)
	trimField(&req.Recipient)

	var out []FieldError
	out = append(out, validateVarchar("idempotency_key", req.IdempotencyKey, true)...)
	out = append(out, validateVarchar("recipient", req.Recipient, true)...)
	out = append(out, validatePriority("priority", req.Priority)...)
	out = append(out, validateOptionalSendAt("send_at", req.SendAt)...)

	pathP := "payload"
	if req.Payload == nil {
		req.Payload = map[string]any{}
	}
	out = append(out, validatePayloadCommon("", req.Payload)...)
	if len(out) > 0 {
		return out
	}

	out = append(out, reservedPayloadErrors(pathP, req.Payload)...)

	if req.TemplateID == nil {
		out = append(out, validateNonTemplateChannelContent("", req.Channel, req.Payload)...)
	} else {
		out = append(out, validateTemplatePayload(pathP, req.Payload)...)
	}
	return out
}

// ValidateBatchSubmitRequest validates the batch envelope and each notification.
// It trims idempotency_key and each recipient. Per-row idempotency is
// idempotency_key + "-" + recipient and must fit VARCHAR(255).
func ValidateBatchSubmitRequest(req *BatchSubmitRequest) []FieldError {
	trimField(&req.IdempotencyKey)

	var out []FieldError
	out = append(out, validateVarchar("idempotency_key", req.IdempotencyKey, true)...)
	if len(req.Notifications) == 0 {
		out = append(out, FieldError{Path: "notifications", Message: "must contain at least one item"})
	}
	if len(out) > 0 {
		return out
	}

	for i := range req.Notifications {
		item := &req.Notifications[i]
		prefix := fmt.Sprintf("notifications[%d].", i)
		trimField(&item.Recipient)

		out = append(out, validateVarchar(prefix+"recipient", item.Recipient, true)...)
		out = append(out, validatePriority(prefix+"priority", item.Priority)...)
		out = append(out, validateOptionalSendAt(prefix+"send_at", item.SendAt)...)

		composite := req.IdempotencyKey + "-" + item.Recipient
		if rl := runeLen(composite); rl > maxDBVarchar {
			out = append(out, FieldError{
				Path: prefix + "recipient",
				Message: fmt.Sprintf("with idempotency_key, combined idempotency (%d characters) exceeds %d; shorten idempotency_key or recipient",
					rl, maxDBVarchar),
			})
		}

		if item.Payload == nil {
			item.Payload = map[string]any{}
		}
		out = append(out, validatePayloadCommon(prefix, item.Payload)...)
		if len(out) > 0 {
			continue
		}

		pp := prefix + "payload"
		out = append(out, reservedPayloadErrors(pp, item.Payload)...)

		if item.TemplateID == nil {
			out = append(out, validateNonTemplateChannelContent(pp, item.Channel, item.Payload)...)
		} else {
			out = append(out, validateTemplatePayload(pp, item.Payload)...)
		}
	}
	return out
}

// --- Normalization helpers ---

func normalizedPriority(p int) int {
	if p == 0 {
		return 5
	}
	return p
}
