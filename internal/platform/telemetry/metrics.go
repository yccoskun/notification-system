package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// We use promauto to automatically register these metrics with the global Prometheus registry.
// They will be exposed automatically when we mount the promhttp.Handler() in our API router.

var (
	// NotificationsReceived tracks the ingress rate (The API edge).
	// Labels allow us to query: "How many high-priority SMS messages did we receive?"
	NotificationsReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_received_total",
			Help: "Total number of notification requests accepted by the API",
		},
		[]string{"channel", "priority"},
	)

	// NotificationsDelivered tracks the egress rate and error rate (The Worker edge).
	// Status labels will be things like "SENT", "FAILED_PROVIDER_500", "FAILED_MAX_RETRIES".
	NotificationsDelivered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_delivered_total",
			Help: "Total number of notifications processed by workers",
		},
		[]string{"channel", "status"},
	)

	// DeliveryLatency tracks the Duration of external provider API calls.
	DeliveryLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "delivery_latency_seconds",
			Help: "Latency of external provider webhook delivery API calls",
			// Buckets carefully tuned for fast API calls, extending out to timeout thresholds.
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"channel"},
	)

	// RateLimitHits tracks how often our distributed Token Bucket blocks a worker.
	// A high climb in this graph means we need to either negotiate a higher limit
	// with our provider, or scale down our worker pool.
	RateLimitHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_hits_total",
			Help: "Total number of times a worker was throttled by the Redis Token Bucket",
		},
		[]string{"channel"},
	)
)
