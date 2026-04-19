package webhook

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateSecret_Format(t *testing.T) {
	secret := generateSecret()

	require.True(t, strings.HasPrefix(secret, "whsec_"), "secret must start with whsec_")

	hexPart := strings.TrimPrefix(secret, "whsec_")
	assert.Len(t, hexPart, 64, "hex part must be 64 chars (32 bytes)")

	_, err := hex.DecodeString(hexPart)
	assert.NoError(t, err, "hex part must be valid hex")
}

func TestGenerateSecret_Uniqueness(t *testing.T) {
	const n = 100
	seen := make(map[string]struct{}, n)
	for range n {
		s := generateSecret()
		_, dup := seen[s]
		require.False(t, dup, "generateSecret must not produce duplicates")
		seen[s] = struct{}{}
	}
}

func TestNewWebhook_SetsFields(t *testing.T) {
	url := "https://example.com/hook"
	events := []string{"user.created", "order.paid"}

	before := time.Now().UTC()
	wh := newWebhook(url, events)
	after := time.Now().UTC()

	assert.True(t, strings.HasPrefix(wh.ID, "wh_"), "ID must be prefixed with wh_")
	assert.Greater(t, len(wh.ID), len("wh_"), "ID must have uuid portion")
	assert.Equal(t, url, wh.Url)
	assert.Equal(t, events, wh.EnabledEvents)
	assert.True(t, wh.Active, "new webhook should be active")
	assert.True(t, strings.HasPrefix(wh.Secret, "whsec_"), "secret must be prefixed")
	assert.Equal(t, time.UTC, wh.CreatedAt.Location(), "CreatedAt must be UTC")
	assert.False(t, wh.CreatedAt.Before(before), "CreatedAt must be >= before")
	assert.False(t, wh.CreatedAt.After(after), "CreatedAt must be <= after")
}
