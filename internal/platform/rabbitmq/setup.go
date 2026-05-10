package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SetupTopology idempotently declares the exact exchange, queue, and bindings
// required for the notification system to function.
func SetupTopology(ch *amqp.Channel) error {
	// 1. Declare the Exchange (Direct routing)
	err := ch.ExchangeDeclare(
		"notifications.exchange", // name
		"direct",                 // type
		true,                     // durable (survives restarts)
		false,                    // auto-deleted
		false,                    // internal
		false,                    // no-wait
		nil,                      // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// 2. Declare the Queue with the x-max-priority argument
	// This is the "magic" that allows high-priority messages to skip the line.
	args := amqp.Table{
		"x-max-priority": int32(10), // Strict type required by RabbitMQ's Erlang VM
	}
	q, err := ch.QueueDeclare(
		"notifications.main", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		args,                 // arguments (PRIORITY ENABLED)
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 3. Bind the Queue to the Exchange
	err = ch.QueueBind(
		q.Name,                   // queue name
		"notifications.main",     // routing key
		"notifications.exchange", // exchange
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	return nil
}
