package domain

import (
	"encoding/json"
	"time"
)

type Webhook struct {
	ID            string    `bson:"_id"`
	Url           string    `bson:"url"`
	EnabledEvents []string  `bson:"enabled_events"`
	Secret        string    `bson:"secret"`
	Active        bool      `bson:"active"`
	CreatedAt     time.Time `bson:"created_at"`
}

type Event struct {
	ID            string            `bson:"_id"`
	WebhookID     string            `bson:"webhook_id"`
	EventType     string            `bson:"event_type"`
	Data          json.RawMessage   `bson:"data"`
	Status        string            `bson:"status"`
	AttemptNumber int               `bson:"attempt_number"`
	NextRetryAt   time.Time         `bson:"next_retry_at,omitempty"`
	Attempts      []DeliveryAttempt `bson:"delivery_attempts"`
	CreatedAt     time.Time         `bson:"created_at"`
}

type DeliveryAttempt struct {
	Status     string    `bson:"status"`
	HTTPStatus *int      `bson:"http_status"`
	Error      string    `bson:"error,omitempty"`
	At         time.Time `bson:"at"`
}
