package telemetry

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// QueueMetricsQuerier is satisfied by the postgres NotificationRepository.
// Defined here to avoid a telemetry → domain/postgres import cycle.
type QueueMetricsQuerier interface {
	CountByStatus(ctx context.Context) (map[string]map[string]int64, error)
}

// QueueDepthCollector is a prometheus.Collector that queries the database on
// every scrape, so the queue-depth gauge is always up-to-date without needing
// a background goroutine to push stale snapshots.
type QueueDepthCollector struct {
	querier QueueMetricsQuerier
	desc    *prometheus.Desc
}

func NewQueueDepthCollector(q QueueMetricsQuerier) *QueueDepthCollector {
	return &QueueDepthCollector{
		querier: q,
		desc: prometheus.NewDesc(
			"notification_queue_depth",
			"Current number of notifications by channel and status (sampled at scrape time)",
			[]string{"channel", "status"},
			nil,
		),
	}
}

func (c *QueueDepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *QueueDepthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	counts, err := c.querier.CountByStatus(ctx)
	if err != nil {
		slog.Error("queue depth collector: failed to query DB", "error", err)
		return
	}

	for channel, statuses := range counts {
		for status, count := range statuses {
			ch <- prometheus.MustNewConstMetric(
				c.desc,
				prometheus.GaugeValue,
				float64(count),
				channel, status,
			)
		}
	}
}
