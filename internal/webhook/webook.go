package webhook

import (
	"crypto/rand"
	"encoding/hex"
	"time"
	"webhook-dispatcher/internal/pkg/domain"

	"github.com/google/uuid"
)

func generateSecret() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	// wh = webhook - sec = secret
	return "whsec_" + hex.EncodeToString(b)
}

func newWebhook(url string, enabledEvents []string) domain.Webhook {
	return domain.Webhook{
		ID:            "wh_" + uuid.New().String(),
		Url:           url,
		EnabledEvents: enabledEvents,
		Secret:        generateSecret(),
		Active:        true,
		CreatedAt:     time.Now().UTC(),
	}
}
