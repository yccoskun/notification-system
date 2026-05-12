package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusUpdateIDFromMessage(t *testing.T) {
	id, ok := statusUpdateIDFromMessage([]byte(`{"id":"9b2c4e1a-0000-4000-8000-000000000001","status":"PENDING","detail":"rate_limited"}`))
	require.True(t, ok)
	require.Equal(t, "9b2c4e1a-0000-4000-8000-000000000001", id)

	_, ok = statusUpdateIDFromMessage([]byte(`{}`))
	require.False(t, ok)

	_, ok = statusUpdateIDFromMessage([]byte(`not json`))
	require.False(t, ok)
}
