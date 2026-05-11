package rabbitmq

import "fmt"

// amqpTableCarrier is a local type that we can define methods on.
// Since amqp.Table is just a map[string]interface{}, we use the same underlying type.
type amqpTableCarrier map[string]interface{}

// Get retrieves a value from the headers.
// We use fmt.Sprintf to safely convert the interface{} to a string for OpenTelemetry.
func (c amqpTableCarrier) Get(key string) string {
	if val, ok := c[key]; ok {
		return fmt.Sprintf("%v", val)
	}
	return ""
}

// Set adds or updates a header value.
func (c amqpTableCarrier) Set(key string, value string) {
	c[key] = value
}

// Keys returns all header names currently in the map.
func (c amqpTableCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
