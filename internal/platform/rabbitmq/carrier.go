package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

// amqpTableCarrier adapts amqp.Table to the OpenTelemetry TextMapCarrier interface.
// This is the bridge that allows trace IDs to cross the message broker network.
type amqpTableCarrier amqp.Table

func (c amqpTableCarrier) Get(key string) string {
	if val, ok := c[key]; ok {
		if s, isString := val.(string); isString {
			return s
		}
	}
	return ""
}

func (c amqpTableCarrier) Set(key string, value string) {
	c[key] = value
}

func (c amqpTableCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
