package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"notification-system/internal/platform/postgres"
	"notification-system/internal/platform/provider"
	"notification-system/internal/platform/rabbitmq"
	redisPlatform "notification-system/internal/platform/redis"
	"notification-system/internal/platform/telemetry"
	"notification-system/internal/service"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracer, err := telemetry.InitTracer(ctx, "worker-service", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if err != nil {
		slog.Error("failed to initialize tracer", "error", err)
	} else {
		defer shutdownTracer(context.Background())
	}
	// 1. Platform Connections
	dbPool := postgres.MustConnect(os.Getenv("DATABASE_URL"))
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_URL")})

	// 2. Dependencies
	repo := postgres.NewNotificationRepository(dbPool)
	tmplRepo := postgres.NewTemplateRepository(dbPool)
	limiter := redisPlatform.NewRateLimiter(rdb)
	idemp := redisPlatform.NewIdempotencyGuard(rdb)
	statusPub := redisPlatform.NewPubSub(rdb)
	webhookProvider := provider.NewWebhookProvider(os.Getenv("PROVIDER_URL"))

	// 3. Orchestrator Service
	deliverySvc := service.NewDeliveryService(
		repo, tmplRepo, limiter, idemp, webhookProvider, statusPub,
	)

	// 4. RabbitMQ Consumer
	_, ch, err := rabbitmq.NewChannel(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		panic(err)
	}
	consumer := rabbitmq.NewConsumer(ch)

	// Start consuming and passing tasks to the orchestrator
	go consumer.Start(ctx, 10, deliverySvc.HandleDelivery)

	<-ctx.Done()
}
