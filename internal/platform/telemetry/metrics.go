package telemetry

import "github.com/prometheus/client_golang/prometheus"

var (
	// NotificationsReceived tracks ingress at the API layer.
	NotificationsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_received_total",
			Help: "Total number of notifications received by the API",
		},
		[]string{"channel", "source"},
	)

	// NotificationsSent tracks delivery outcomes (success / error) at the worker layer.
	NotificationsSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_sent_total",
			Help: "Total number of notifications processed by the worker",
		},
		[]string{"channel", "status"},
	)

	// RateLimitHits counts how many deliveries were deferred due to per-recipient rate limits.
	RateLimitHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_rate_limited_total",
			Help: "Total number of notifications deferred by the rate limiter",
		},
		[]string{"channel"},
	)

	// DeliveryLatency measures how long the external provider HTTP call takes.
	// Observed once per attempt inside WebhookProvider.Send (the authoritative place).
	DeliveryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "notification_delivery_duration_seconds",
			Help:    "End-to-end latency of an external provider HTTP call, in seconds",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"channel"},
	)

	// HTTPRequestDuration tracks API handler latency, labelled by method, route, and status code.
	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests served by the API, in seconds",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestsInFlight is the number of HTTP requests currently being handled.
	HTTPRequestsInFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Current number of HTTP requests being processed by the API",
		},
	)
)

func init() {
	prometheus.MustRegister(
		NotificationsReceived,
		NotificationsSent,
		RateLimitHits,
		DeliveryLatency,
		HTTPRequestDuration,
		HTTPRequestsInFlight,
	)
}
