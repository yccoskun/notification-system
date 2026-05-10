package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InitTracer bootstraps the OpenTelemetry pipeline.
// It returns a shutdown function that MUST be deferred in main().
func InitTracer(ctx context.Context, serviceName, endpoint string) (func(context.Context) error, error) {
	// 1. Set up the OTLP Exporter (gRPC) to stream spans to Jaeger
	// We use insecure credentials because this runs inside our trusted docker bridge network
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// 2. Define the Resource (The identity of the microservice)
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// 3. Configure the Tracer Provider
	// We use a BatchSpanProcessor so we don't block application logic while sending telemetry
	bsp := sdktrace.NewBatchSpanProcessor(exporter)
	tracerProvider := sdktrace.NewTracerProvider(
		// In production with billions of requests, we would use ParentBased(TraceIDRatioBased)
		// to sample e.g. 5% of traffic. For this system, we trace everything.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// 4. Register Globals
	otel.SetTracerProvider(tracerProvider)

	// 5. The Magic: Register Global Propagator
	// THIS IS CRITICAL. Without W3C TraceContext, Trace IDs will not cross
	// the RabbitMQ network boundary, resulting in disconnected, orphaned traces.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Return the clean shutdown function
	return tracerProvider.Shutdown, nil
}
