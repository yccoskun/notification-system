package telemetry

import "github.com/prometheus/client_golang/prometheus"

var (
	// NotificationsReceived tracks ingress
	NotificationsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_received_total",
			Help: "Total number of notifications received by the API",
		},
		[]string{"channel", "source"},
	)

	// NotificationsSent tracks egress outcomes
	NotificationsSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_sent_total",
			Help: "Total number of notifications processed by the worker",
		},
		[]string{"channel", "status"},
	)

	// RateLimitHits tracks throttled requests
	RateLimitHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_rate_limited_total",
			Help: "Total number of notifications throttled by rate limiter",
		},
		[]string{"channel"},
	)

	// DeliveryLatency tracks how long the external provider takes
	DeliveryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "notification_delivery_duration_seconds",
			Help:    "Latency of external provider delivery in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"channel"},
	)
)

func init() {
	prometheus.MustRegister(NotificationsReceived)
	prometheus.MustRegister(NotificationsSent)
	prometheus.MustRegister(RateLimitHits)
	prometheus.MustRegister(DeliveryLatency)
}
