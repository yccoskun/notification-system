package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// NewChannel is a helper to get a ready-to-use channel from a URL.
func NewChannel(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return conn, ch, nil
}
