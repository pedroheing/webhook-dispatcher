package webhook_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"webhook-dispatcher/internal/pkg/domain"
	"webhook-dispatcher/internal/webhook"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func activeWebhook() domain.Webhook {
	return domain.Webhook{
		ID:            "wh_1",
		Url:           "https://example.com/hook",
		Secret:        "whsec_x",
		EnabledEvents: []string{"user.created"},
		Active:        true,
	}
}

func validEventReq() webhook.CreateEventRequest {
	return webhook.CreateEventRequest{
		EventType: "user.created",
		Data:      json.RawMessage(`{"user_id":"u1"}`),
	}
}

func TestCreateEvent_Success(t *testing.T) {
	engine, repo, writer := newTestEngine(t)
	wh := activeWebhook()

	repo.EXPECT().Get(mock.Anything, wh.ID).Return(wh, nil)

	var capturedEvent domain.Event
	repo.EXPECT().CreateEvent(mock.Anything, mock.Anything).
		Run(func(_ context.Context, ev domain.Event) { capturedEvent = ev }).
		Return(nil)

	var capturedMsgs []kafka.Message
	writer.EXPECT().WriteMessages(mock.Anything, mock.Anything).
		Run(func(_ context.Context, msgs ...kafka.Message) { capturedMsgs = msgs }).
		Return(nil)

	rec := doJSON(engine, http.MethodPost, "/webhooks/"+wh.ID+"/events", validEventReq())

	require.Equal(t, http.StatusCreated, rec.Code, "body=%s", rec.Body.String())

	var resp webhook.CreateEventResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, strings.HasPrefix(resp.ID, "ev_"))

	assert.Equal(t, resp.ID, capturedEvent.ID)
	assert.Equal(t, wh.ID, capturedEvent.WebhookID)
	assert.Equal(t, "user.created", capturedEvent.EventType)

	require.Len(t, capturedMsgs, 1)
	assert.Equal(t, "events.pending", capturedMsgs[0].Topic)
	assert.Equal(t, resp.ID, string(capturedMsgs[0].Key))
}

func TestCreateEvent_WebhookNotFound(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	repo.EXPECT().Get(mock.Anything, "wh_missing").Return(domain.Webhook{}, mongo.ErrNoDocuments)

	rec := doJSON(engine, http.MethodPost, "/webhooks/wh_missing/events", validEventReq())

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "webhook not found")
}

func TestCreateEvent_WebhookInactive(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	wh := activeWebhook()
	wh.Active = false
	repo.EXPECT().Get(mock.Anything, wh.ID).Return(wh, nil)

	rec := doJSON(engine, http.MethodPost, "/webhooks/"+wh.ID+"/events", validEventReq())

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateEvent_EventTypeNotAccepted(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	wh := activeWebhook()
	repo.EXPECT().Get(mock.Anything, wh.ID).Return(wh, nil)

	rec := doJSON(engine, http.MethodPost, "/webhooks/"+wh.ID+"/events", webhook.CreateEventRequest{
		EventType: "order.paid", // not in EnabledEvents
		Data:      json.RawMessage(`{}`),
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "does not accept event type")
}

func TestCreateEvent_BadRequestOnInvalidBody(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	rec := doJSON(engine, http.MethodPost, "/webhooks/wh_1/events", map[string]any{
		// missing event_type and data
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateEvent_RepoCreateError(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	wh := activeWebhook()
	repo.EXPECT().Get(mock.Anything, wh.ID).Return(wh, nil)
	repo.EXPECT().CreateEvent(mock.Anything, mock.Anything).Return(errors.New("insert failed"))

	rec := doJSON(engine, http.MethodPost, "/webhooks/"+wh.ID+"/events", validEventReq())

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestCreateEvent_KafkaError(t *testing.T) {
	engine, repo, writer := newTestEngine(t)
	wh := activeWebhook()
	repo.EXPECT().Get(mock.Anything, wh.ID).Return(wh, nil)
	repo.EXPECT().CreateEvent(mock.Anything, mock.Anything).Return(nil)
	writer.EXPECT().WriteMessages(mock.Anything, mock.Anything).Return(errors.New("kafka down"))

	rec := doJSON(engine, http.MethodPost, "/webhooks/"+wh.ID+"/events", validEventReq())

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}
