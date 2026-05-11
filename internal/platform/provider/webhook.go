package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"notification-system/internal/domain"
	"notification-system/internal/platform/circuitbreaker"
	"notification-system/internal/platform/telemetry"
)

// WebhookProvider simulates Twilio/SendGrid by posting to a webhook URL.
type WebhookProvider struct {
	client  *http.Client
	url     string
	breaker *circuitbreaker.Breaker
}

func NewWebhookProvider(url string) *WebhookProvider {
	return &WebhookProvider{
		client: &http.Client{
			Timeout: 5 * time.Second, // Never hang forever
			// Inject OTel transport to automatically propagate traces into outbound HTTP headers
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
		url:     url,
		breaker: circuitbreaker.NewBreaker("external_webhook_provider"),
	}
}

// Send attempts to deliver the notification.
func (p *WebhookProvider) Send(ctx context.Context, n *domain.Notification) error {
	// 1. Prometheus Metric: Record latency regardless of success/failure
	start := time.Now()
	defer func() {
		telemetry.DeliveryLatency.WithLabelValues(string(n.Channel)).Observe(time.Since(start).Seconds())
	}()

	result, err := p.breaker.Execute(func() (any, error) {
		// Construct the final JSON payload for the external API
		finalPayload := make(map[string]any)

		if rendered, ok := n.Payload["_rendered_content"].(string); ok {
			// This is what the external API actually wants to see
			finalPayload["message"] = rendered
			if subject, ok := n.Payload["_rendered_subject"].(string); ok {
				finalPayload["subject"] = subject
			}
		} else {
			// Fallback: If no template was used, send the raw payload
			finalPayload = n.Payload
		}

		payloadBytes, err := json.Marshal(finalPayload)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := p.client.Do(req)
		if err != nil {
			// Network timeout, DNS failure, etc. Trips the breaker.
			return nil, err
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode >= 500 {
			// Provider is down. Trips the breaker.
			return nil, fmt.Errorf("provider 5xx error: %s", string(body))
		}

		if resp.StatusCode >= 400 {
			// Client Error (Bad payload).
			// WE DO NOT RETURN AN ERROR HERE. If we do, the breaker trips.
			// Instead, we return the error as the "successful" payload of the breaker.
			return fmt.Errorf("provider 4xx error (do not retry): %s", string(body)), nil
		}

		return nil, nil // 2xx Success
	})

	// 3. Handle Breaker/5xx Errors
	if err != nil {
		return err // Could be gobreaker.ErrOpenState or the 500 error
	}

	// 4. Handle 4xx Client Errors safely extracted from the breaker
	if result != nil {
		if clientErr, ok := result.(error); ok {
			return clientErr // Return the 400 error to the worker so it can fail the message permanently
		}
	}

	return nil
}
