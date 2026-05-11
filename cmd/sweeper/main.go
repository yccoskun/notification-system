package main

import (
	"context"
	"fmt"
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
	_, ch, err := rabbitmq.NewChannel(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		panic(err)
	}
	if err := rabbitmq.SetupTopology(ch); err != nil {
		panic(fmt.Sprintf("failed to setup rabbitmq topology: %v", err))
	}
	rmqPub := rabbitmq.NewPublisher(ch)
	repo := postgres.NewNotificationRepository(dbPool)

	// 100 rows per sweep, checking every 30 seconds
	sweeper := service.NewSweeper(repo, rmqPub, 100)

	sweeper.Start(ctx, 30*time.Second)
}
