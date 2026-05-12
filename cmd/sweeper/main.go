package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification-system/internal/platform/postgres"
	"notification-system/internal/platform/rabbitmq"
	"notification-system/internal/platform/telemetry"
	"notification-system/internal/service"
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

	dbPool := postgres.MustConnect(os.Getenv("DATABASE_URL"))
	rmqPub, err := rabbitmq.NewResilientPublisher(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		panic(err)
	}
	defer func() { _ = rmqPub.Close() }()
	repo := postgres.NewNotificationRepository(dbPool)

	// 100 rows per sweep, checking every 30 seconds
	sweeper := service.NewSweeper(repo, rmqPub, 100)

	sweeper.Start(ctx, 30*time.Second)
}
