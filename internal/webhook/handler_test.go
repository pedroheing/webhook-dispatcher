package webhook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"webhook-dispatcher/internal/pkg/domain"
	"webhook-dispatcher/internal/webhook"
	"webhook-dispatcher/internal/webhook/mocks"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func newTestEngine(t *testing.T) (*gin.Engine, *mocks.MockRepository, *mocks.MockKafkaWriter) {
	t.Helper()
	repo := mocks.NewMockRepository(t)
	writer := mocks.NewMockKafkaWriter(t)
	handler := webhook.NewHandler(writer, repo, webhook.Config{PendingEventsTopic: "events.pending"})
	engine := gin.New()
	handler.RegisterRoutes(engine)
	return engine, repo, writer
}

func doJSON(engine *gin.Engine, method, path string, body any) *httptest.ResponseRecorder {
	var buf *bytes.Buffer
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	return rec
}

func TestHandlerCreate_Success(t *testing.T) {
	engine, repo, _ := newTestEngine(t)

	var captured domain.Webhook
	repo.EXPECT().Create(mock.Anything, mock.Anything).
		Run(func(_ context.Context, wh domain.Webhook) { captured = wh }).
		Return(nil)

	rec := doJSON(engine, http.MethodPost, "/webhooks", webhook.CreateWebhookRequest{
		Url:           "https://example.com/hook",
		EnabledEvents: []string{"user.created"},
	})

	require.Equal(t, http.StatusCreated, rec.Code)

	var resp webhook.CreateWebhookResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, strings.HasPrefix(resp.ID, "wh_"))
	assert.True(t, strings.HasPrefix(resp.Secret, "whsec_"))
	assert.True(t, resp.Active)
	assert.Equal(t, "https://example.com/hook", resp.Url)

	assert.Equal(t, "https://example.com/hook", captured.Url)
	assert.Equal(t, []string{"user.created"}, captured.EnabledEvents)
	assert.True(t, captured.Active)
}

func TestHandlerCreate_BadRequestOnInvalidBody(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	rec := doJSON(engine, http.MethodPost, "/webhooks", map[string]any{
		"url": "not-a-valid-url",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerCreate_RepoError(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	repo.EXPECT().Create(mock.Anything, mock.Anything).Return(errors.New("mongo down"))

	rec := doJSON(engine, http.MethodPost, "/webhooks", webhook.CreateWebhookRequest{
		Url:           "https://example.com/hook",
		EnabledEvents: []string{"user.created"},
	})

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandlerGet_NotFound(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	repo.EXPECT().Get(mock.Anything, "wh_missing").Return(domain.Webhook{}, mongo.ErrNoDocuments)

	rec := doJSON(engine, http.MethodGet, "/webhooks/wh_missing", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerGet_Success(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	wh := domain.Webhook{ID: "wh_1", Url: "https://x.com", Active: true, EnabledEvents: []string{"user.created"}, Secret: "whsec_dont_leak"}
	repo.EXPECT().Get(mock.Anything, "wh_1").Return(wh, nil)

	rec := doJSON(engine, http.MethodGet, "/webhooks/wh_1", nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp webhook.WebhookResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "wh_1", resp.ID)
	assert.Equal(t, "https://x.com", resp.Url)
	assert.NotContains(t, rec.Body.String(), "secret")
}

func TestHandlerList_EmptyReturnsEmptyArray(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	repo.EXPECT().List(mock.Anything).Return(nil, nil)

	rec := doJSON(engine, http.MethodGet, "/webhooks", nil)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "[]", strings.TrimSpace(rec.Body.String()))
}

func TestHandlerList_Success(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	whs := []domain.Webhook{
		{ID: "wh_1", Url: "https://a.com", Active: true, Secret: "whsec_a"},
		{ID: "wh_2", Url: "https://b.com", Active: false, Secret: "whsec_b"},
	}
	repo.EXPECT().List(mock.Anything).Return(whs, nil)

	rec := doJSON(engine, http.MethodGet, "/webhooks", nil)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp []webhook.WebhookResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp, 2)
	assert.NotContains(t, rec.Body.String(), "secret")
}

func TestHandlerUpdate_NotFound(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	repo.EXPECT().Update(mock.Anything, "wh_missing", mock.Anything).
		Return(domain.Webhook{}, mongo.ErrNoDocuments)

	active := false
	rec := doJSON(engine, http.MethodPut, "/webhooks/wh_missing", webhook.UpdateWebhookRequest{Active: &active})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerUpdate_Success(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	updated := domain.Webhook{ID: "wh_1", Url: "https://x.com", Active: false}
	repo.EXPECT().Update(mock.Anything, "wh_1", mock.MatchedBy(func(p webhook.WebhookPatch) bool {
		return p.Active != nil && !*p.Active
	})).Return(updated, nil)

	active := false
	rec := doJSON(engine, http.MethodPut, "/webhooks/wh_1", webhook.UpdateWebhookRequest{Active: &active})

	require.Equal(t, http.StatusOK, rec.Code)
	var resp webhook.WebhookResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.False(t, resp.Active)
}

func TestHandlerUpdate_BadRequestOnInvalidBody(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	badURL := "not-a-url"
	rec := doJSON(engine, http.MethodPut, "/webhooks/wh_1", webhook.UpdateWebhookRequest{Url: &badURL})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerDelete_NotFound(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	repo.EXPECT().Delete(mock.Anything, "wh_missing").Return(mongo.ErrNoDocuments)

	rec := doJSON(engine, http.MethodDelete, "/webhooks/wh_missing", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerDelete_Success(t *testing.T) {
	engine, repo, _ := newTestEngine(t)
	repo.EXPECT().Delete(mock.Anything, "wh_1").Return(nil)

	rec := doJSON(engine, http.MethodDelete, "/webhooks/wh_1", nil)

	assert.Equal(t, http.StatusOK, rec.Code)
}
