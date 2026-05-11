package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification-system/internal/api"
	"notification-system/internal/platform/postgres"
	"notification-system/internal/platform/rabbitmq"
	redisPlatform "notification-system/internal/platform/redis"
	"notification-system/internal/platform/telemetry"

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

	// DB Migrations
	dbURL := os.Getenv("DATABASE_URL")
	slog.Info("checking database migrations...")
	if err := postgres.RunMigrations(dbURL); err != nil {
		// We panic here because if migrations fail,
		// the app's repositories will crash anyway.
		panic(fmt.Sprintf("migration failed: %v", err))
	}

	// 1. Platform Setup (DB, Redis, RMQ)
	dbPool := postgres.MustConnect(dbURL)
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_URL")})
	_, ch, err := rabbitmq.NewChannel(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		panic(err)
	}
	if err := rabbitmq.SetupTopology(ch); err != nil {
		panic(fmt.Sprintf("failed to setup rabbitmq topology: %v", err))
	}
	rmqPub := rabbitmq.NewPublisher(ch)

	// 2. Repository & Publisher Init
	repo := postgres.NewNotificationRepository(dbPool)
	redisPubSub := redisPlatform.NewPubSub(rdb)

	// 3. WebSocket Hub Init
	hub := api.NewWSHub()
	go hub.Run()
	go hub.ListenRedis(ctx, redisPubSub.Subscribe(ctx))

	// 4. API Handler & Router
	handler := api.NewNotificationHandler(repo, rmqPub)
	router := api.NewRouter("api-service", handler, hub)

	// 5. Graceful Server Startup
	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("listen", "error", err)
		}
	}()

	slog.Info("API Server started on :8080")
	<-ctx.Done()
	slog.Info("Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server forced to shutdown", "error", err)
	}
	if shutdownTracer != nil {
		if err := shutdownTracer(context.Background()); err != nil {
			slog.Error("failed to shutdown tracer", "error", err)
		}
	}
}
