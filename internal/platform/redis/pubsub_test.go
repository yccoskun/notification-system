package redis

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusUpdate_JSONRoundTrip(t *testing.T) {
	u := StatusUpdate{ID: "9b2c4e1a-0000-4000-8000-000000000001", Status: "PENDING", Detail: "rate_limited"}
	b, err := json.Marshal(u)
	require.NoError(t, err)

	var out StatusUpdate
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, u, out)
}
