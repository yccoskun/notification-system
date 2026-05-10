package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rmqcontainer "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	myrmq "notification-system/internal/platform/rabbitmq"
)

func setupTestRabbitMQ(t *testing.T) (*amqp.Connection, func()) {
	ctx := context.Background()

	// 1. Ephemeral RabbitMQ Container
	container, err := rmqcontainer.Run(ctx,
		"rabbitmq:3.13-management-alpine",
		rmqcontainer.WithAdminUsername("admin"),
		rmqcontainer.WithAdminPassword("secret"),
	)
	require.NoError(t, err)

	amqpURL, err := container.AmqpURL(ctx)
	require.NoError(t, err)

	// 2. Connect
	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err)

	teardown := func() {
		conn.Close()
		_ = container.Terminate(ctx)
	}

	return conn, teardown
}

func TestRabbitMQ_PubSubWithContext(t *testing.T) {
	conn, teardown := setupTestRabbitMQ(t)
	defer teardown()

	// Ensure OTel Propagator is globally set for the test
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// Setup Infrastructure
	err = myrmq.SetupTopology(ch)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	publisher := myrmq.NewPublisher(ch)
	consumer := myrmq.NewConsumer(ch)

	testNotificationID := uuid.New()
	testPriority := 10

	// We use a Go channel to synchronize the async consumer with our test execution
	msgReceived := make(chan uuid.UUID, 1)

	// 1. Start the Consumer Worker Pool
	err = consumer.Start(ctx, 1, func(handlerCtx context.Context, id uuid.UUID) error {
		// Prove that the handler was called with the exact ID we published
		msgReceived <- id
		return nil
	})
	require.NoError(t, err)

	// 2. Publish the Message
	err = publisher.Publish(ctx, testNotificationID, testPriority)
	require.NoError(t, err)

	// 3. Await receipt or timeout
	select {
	case receivedID := <-msgReceived:
		assert.Equal(t, testNotificationID, receivedID, "Received ID should match Published ID")
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for RabbitMQ message to be consumed")
	}
}
