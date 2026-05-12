package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"notification-system/internal/domain"
	"notification-system/internal/platform/postgres"
	"notification-system/internal/platform/provider"
	"notification-system/internal/platform/rabbitmq"
	redisPlatform "notification-system/internal/platform/redis"
	"notification-system/internal/platform/telemetry"
	"notification-system/internal/service"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracer, err := telemetry.InitTracer(ctx, "api-service", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if err != nil {
		slog.Error("failed to initialize tracer", "error", err)
	} else {
		defer func() {
			if err := shutdownTracer(context.Background()); err != nil {
				slog.Error("failed to shutdown tracer", "error", err)
			}
		}()
	}
	// 1. Platform Connections
	dbPool := postgres.MustConnect(os.Getenv("DATABASE_URL"))
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_URL")})

	// 2. Dependencies
	repo := postgres.NewNotificationRepository(dbPool)
	prometheus.MustRegister(telemetry.NewQueueDepthCollector(repo))
	tmplRepo := postgres.NewTemplateRepository(dbPool)
	limiter := redisPlatform.NewRateLimiter(rdb)
	idemp := redisPlatform.NewIdempotencyGuard(rdb)
	statusPub := redisPlatform.NewPubSub(rdb)

	emailURL := os.Getenv("EMAIL_PROVIDER_URL")
	smsURL := os.Getenv("SMS_PROVIDER_URL")
	pushURL := os.Getenv("PUSH_PROVIDER_URL")

	providers := map[domain.ChannelType]service.Provider{
		domain.ChannelEmail: provider.NewWebhookProvider(emailURL),
		domain.ChannelSMS:   provider.NewWebhookProvider(smsURL),
		domain.ChannelPush:  provider.NewWebhookProvider(pushURL),
	}

	deliverySvc := service.NewDeliveryService(
		repo, tmplRepo, limiter, idemp, providers, statusPub,
	)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		slog.Info("Worker metrics started on :8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			slog.Error("metrics server failed", "error", err)
		}
	}()

	// 4. RabbitMQ consumer (reconnects on broker / TCP failure)
	go func() {
		if err := rabbitmq.RunResilientConsumer(ctx, os.Getenv("RABBITMQ_URL"), 10, deliverySvc.HandleDelivery); err != nil {
			slog.Error("rabbitmq consumer stopped", "error", err)
		}
	}()

	<-ctx.Done()
	if err := shutdownTracer(context.Background()); err != nil {
		slog.Error("failed to shutdown tracer", "error", err)
	}
}
