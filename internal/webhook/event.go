package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"webhook-dispatcher/internal/pkg/domain"
	"webhook-dispatcher/internal/pkg/httpx"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type CreateEventRequest struct {
	EventType string          `json:"event_type" binding:"required"`
	Data      json.RawMessage `json:"data" binding:"required"`
}

type CreateEventResponse struct {
	ID string `json:"id"`
}

var (
	ErrWebhookInactive      = errors.New("webhook is inactive")
	ErrEventTypeNotAccepted = errors.New("event type not accepted")
)

func (h *Handler) CreateEvent(c *gin.Context) {
	var req CreateEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httpx.BadRequest(c, err)
		return
	}

	webhookId := c.Param(":id")

	webhook, err := h.validateWebhook(c.Request.Context(), req, webhookId)
	if err != nil {
		h.handleWebhookError(c, err, req, webhookId)
		return
	}

	event := domain.Event{
		ID:        "ev_" + uuid.New().String(),
		WebhookID: webhook.ID,
		EventType: req.EventType,
		Data:      req.Data,
	}

	if err := h.publishEvent(c.Request.Context(), event); err != nil {
		log.Printf("publish event failed: %v", err)
		httpx.InternalError(c)
		return
	}

	httpx.Created(c, CreateEventResponse{ID: event.ID})
}

func (h *Handler) validateWebhook(ctx context.Context, req CreateEventRequest, webhookId string) (domain.Webhook, error) {
	webhook, err := h.repository.Get(ctx, webhookId)
	if err != nil {
		return domain.Webhook{}, err
	}
	if !webhook.Active {
		return domain.Webhook{}, ErrWebhookInactive
	}
	if !slices.Contains(webhook.EnabledEvents, req.EventType) {
		return domain.Webhook{}, ErrEventTypeNotAccepted
	}
	return webhook, nil
}

func (h *Handler) handleWebhookError(c *gin.Context, err error, req CreateEventRequest, webhookId string) {
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		httpx.BadRequest(c, fmt.Errorf("webhook not found: %s", webhookId))
	case errors.Is(err, ErrWebhookInactive):
		httpx.BadRequest(c, err)
	case errors.Is(err, ErrEventTypeNotAccepted):
		httpx.BadRequest(c, fmt.Errorf("webhook does not accept event type: %s", req.EventType))
	default:
		log.Printf("error finding webhook: %v", err)
		httpx.InternalError(c)
	}
}

func (h *Handler) publishEvent(ctx context.Context, event domain.Event) error {
	if err := h.repository.CreateEvent(ctx, event); err != nil {
		return err
	}
	payload, _ := json.Marshal(event)
	return h.writer.WriteMessages(ctx, kafka.Message{
		Topic: h.config.PendingEventsTopic,
		Key:   []byte(event.ID),
		Value: payload,
	})
}
