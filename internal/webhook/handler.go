package webhook

import (
	"errors"
	"log"
	"webhook-dispatcher/internal/pkg/domain"
	"webhook-dispatcher/internal/pkg/httpx"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type CreateWebhookRequest struct {
	Url           string   `json:"url" binding:"required,url"`
	EnabledEvents []string `json:"enabled_events" binding:"required"`
}

type CreateWebhookResponse struct {
	ID            string   `json:"id"`
	Url           string   `json:"url"`
	EnabledEvents []string `json:"enabled_events"`
	Active        bool     `json:"active"`
	Secret        string   `json:"secret"`
}

type UpdateWebhookRequest struct {
	Url           *string  `json:"url"  binding:"url"`
	EnabledEvents []string `json:"enabled_events"`
	Active        *bool    `json:"active"`
}

type WebhookResponse struct {
	ID            string   `json:"id"`
	Url           string   `json:"url"`
	EnabledEvents []string `json:"enabled_events"`
	Active        bool     `json:"active"`
}

type Handler struct {
	writer     *kafka.Writer
	repository *Repository
}

func NewHandler(writer *kafka.Writer, repository *Repository) *Handler {
	return &Handler{writer: writer, repository: repository}
}

func (h *Handler) RegisterRoutes(r gin.IRouter) {
	webhooks := r.Group("/webhooks")
	webhooks.POST("", h.Create)
	webhooks.GET("", h.List)
	webhooks.GET("/:id", h.Get)
	webhooks.PUT("/:id", h.Update)
	webhooks.DELETE("/:id", h.Delete)
	webhooks.POST(":id/events", h.CreateEvent)
}

func (h *Handler) Create(c *gin.Context) {
	var req CreateWebhookRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httpx.BadRequest(c, err)
		return
	}
	wh := newWebhook(req.Url, req.EnabledEvents)

	if err := h.repository.Create(c.Request.Context(), wh); err != nil {
		log.Printf("repository.Get failed: %v", err)
		httpx.InternalError(c)
		return
	}
	httpx.Created(c, toWebhookCreateResponse(wh))
}

func (h *Handler) Get(c *gin.Context) {
	id := c.Param("id")
	wh, err := h.repository.Get(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			httpx.NotFound(c, "webhook not found")
			return
		}
		log.Printf("repository.Get failed: %v", err)
		httpx.InternalError(c)
		return
	}
	httpx.OK(c, toWebhookResponse(wh))
}

func (h *Handler) List(c *gin.Context) {
	whs, err := h.repository.List(c.Request.Context())
	if err != nil {
		log.Printf("repository.List failed: %v", err)
		httpx.InternalError(c)
		return
	}
	response := make([]WebhookResponse, 0, len(whs))
	for _, wh := range whs {
		response = append(response, toWebhookResponse(wh))
	}
	httpx.OK(c, response)
}

func (h *Handler) Update(c *gin.Context) {
	var req UpdateWebhookRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httpx.BadRequest(c, err)
		return
	}
	id := c.Param("id")
	updated, err := h.repository.Update(c.Request.Context(), id, WebhookPatch{
		Url:           req.Url,
		EnabledEvents: req.EnabledEvents,
		Active:        req.Active,
	})
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			httpx.NotFound(c, "webhook not found")
			return
		}
		log.Printf("repository.Update failed: %v", err)
		httpx.InternalError(c)
		return
	}

	httpx.OK(c, toWebhookResponse(updated))
}

func (h *Handler) Delete(c *gin.Context) {
	id := c.Param("id")
	if err := h.repository.Delete(c.Request.Context(), id); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			httpx.NotFound(c, "webhook not found")
			return
		}
		log.Printf("repository.Delete failed: %v", err)
		httpx.InternalError(c)
		return
	}
	httpx.OKEmpty(c)
}

func toWebhookCreateResponse(wh domain.Webhook) CreateWebhookResponse {
	return CreateWebhookResponse{
		ID:            wh.ID,
		Url:           wh.Url,
		EnabledEvents: wh.EnabledEvents,
		Active:        wh.Active,
		Secret:        wh.Secret,
	}
}

func toWebhookResponse(wh domain.Webhook) WebhookResponse {
	return WebhookResponse{
		ID:            wh.ID,
		Url:           wh.Url,
		EnabledEvents: wh.EnabledEvents,
		Active:        wh.Active,
	}
}
