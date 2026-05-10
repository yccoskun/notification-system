package backoff

import (
	"math/rand"
	"time"
)

const (
	baseDelay = 2 * time.Second
	maxDelay  = 1 * time.Hour
)

// Calculate determines the delay for the next retry attempt using
// exponential backoff with "Full Jitter".
// Formula: Delay = random(0, min(MaxDelay, Base * 2^Attempt))
func Calculate(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	// Calculate exponential growth: Base * 2^attempt
	// We cap the shift to prevent integer overflow on massive attempt counts
	shift := attempt
	if shift > 30 {
		shift = 30
	}

	exp := float64(baseDelay) * float64(int(1)<<shift)

	if exp > float64(maxDelay) {
		exp = float64(maxDelay)
	}

	// Apply Full Jitter: pick a random duration between 0 and the exponential max
	// This prevents all workers from retrying at the exact same millisecond
	jittered := rand.Float64() * exp

	return time.Duration(jittered)
}
