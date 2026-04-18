package event

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
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Handler struct {
	writer *kafka.Writer
	db     *mongo.Database
}

type CreateEventRequest struct {
	WebhookID string          `json:"webhook_id" binding:"required"`
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

func NewHandler(writer *kafka.Writer, db *mongo.Database) *Handler {
	return &Handler{writer: writer, db: db}
}

func (h *Handler) RegisterRoutes(r gin.IRouter) {
	events := r.Group("/events")
	events.POST("", h.Create)
}

func (h *Handler) Create(c *gin.Context) {
	var req CreateEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httpx.BadRequest(c, err)
		return
	}

	webhook, err := h.validateWebhook(c.Request.Context(), req)
	if err != nil {
		h.handleWebhookError(c, err, req)
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

func (h *Handler) validateWebhook(ctx context.Context, req CreateEventRequest) (domain.Webhook, error) {
	var webhook domain.Webhook
	err := h.db.Collection("webhooks").FindOne(ctx, bson.M{
		"_id": req.WebhookID,
	}).Decode(&webhook)
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

func (h *Handler) handleWebhookError(c *gin.Context, err error, req CreateEventRequest) {
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		httpx.BadRequest(c, fmt.Errorf("webhook not found: %s", req.WebhookID))
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
	if _, err := h.db.Collection("events").InsertOne(ctx, event); err != nil {
		return err
	}
	payload, _ := json.Marshal(event)
	return h.writer.WriteMessages(ctx, kafka.Message{
		Topic: "events.pending",
		Key:   []byte(event.ID),
		Value: payload,
	})
}
