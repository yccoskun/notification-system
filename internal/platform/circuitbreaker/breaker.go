package circuitbreaker

import (
	"fmt"
	"time"

	"github.com/sony/gobreaker"
)

// Breaker wraps the standard circuit breaker to encapsulate our domain-specific tuning.
type Breaker struct {
	cb *gobreaker.CircuitBreaker
}

// NewBreaker initializes a circuit breaker with strict failure thresholds.
func NewBreaker(name string) *Breaker {
	st := gobreaker.Settings{
		Name:        name,
		MaxRequests: 1,                // Allow exactly 1 request through in "Half-Open" state to test recovery
		Interval:    0,                // Never clear internal failure counts automatically while Closed
		Timeout:     30 * time.Second, // Wait 30 seconds before transitioning from Open -> Half-Open
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Trip the breaker if we have at least 10 requests AND >50% of them failed
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 10 && failureRatio >= 0.5
		},
	}

	return &Breaker{
		cb: gobreaker.NewCircuitBreaker(st),
	}
}

// Execute runs the provided action. If the circuit is Open, it immediately returns gobreaker.ErrOpenState.
func (b *Breaker) Execute(action func() (any, error)) (any, error) {
	result, err := b.cb.Execute(action)
	if err != nil {
		if err == gobreaker.ErrOpenState {
			return nil, fmt.Errorf("circuit breaker [%s] is open: fast-failing request", b.cb.Name())
		}
		return nil, err
	}
	return result, nil
}
