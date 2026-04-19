package dispatch

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
	"webhook-dispatcher/internal/dispatch/mocks"
	"webhook-dispatcher/internal/pkg/domain"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/time/rate"
)

func baseConfig() Config {
	return Config{
		Workers:         1,
		BufferSize:      1,
		RetentionWindow: 72 * time.Hour,
		CircuitBreakerOptions: CircuitBreakerOptions{
			MaxRequests:        1,
			Interval:           60 * time.Second,
			Timeout:            60 * time.Second,
			FailuresBeforeOpen: 5,
		},
		LimiterOptions: LimiterOptions{
			RefillRate: rate.Inf,
			BucketSize: 100,
		},
		BackoffOptions: BackoffOptions{
			Base:       100 * time.Millisecond,
			MaxDelay:   6 * time.Hour,
			Multiplier: 2,
		},
		HttpOptions: HttpOptions{Timeout: 5 * time.Second},
	}
}

func sampleEvent() domain.Event {
	return domain.Event{
		ID:            "ev_1",
		WebhookID:     "wh_1",
		EventType:     "user.created",
		Data:          json.RawMessage(`{"user_id":"u1"}`),
		Status:        domain.EventStatusPending,
		AttemptNumber: 0,
		CreatedAt:     time.Now().UTC(),
	}
}

type capturedRequest struct {
	headers http.Header
	body    []byte
}

func newHookServer(status int, capture *capturedRequest) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if capture != nil {
			capture.headers = r.Header.Clone()
			capture.body = body
		}
		w.WriteHeader(status)
	}))
}

func activeWebhook(url string) domain.Webhook {
	return domain.Webhook{
		ID:            "wh_1",
		Url:           url,
		Secret:        "sekret",
		EnabledEvents: []string{"user.created"},
		Active:        true,
		CreatedAt:     time.Now().UTC(),
	}
}

// pure fn tests

func TestCalculateBackoff_GrowsExponentiallyWithinJitter(t *testing.T) {
	d := &Dispatcher{config: baseConfig()}

	for attempt := 1; attempt <= 4; attempt++ {
		nominal := float64(d.config.BackoffOptions.Base) * pow(d.config.BackoffOptions.Multiplier, float64(attempt))
		lower := time.Duration(nominal * 0.9)
		upper := time.Duration(nominal * 1.1)

		for range 20 {
			got := d.calculateBackoff(attempt)
			assert.GreaterOrEqual(t, got, lower, "attempt=%d", attempt)
			assert.LessOrEqual(t, got, upper, "attempt=%d", attempt)
		}
	}
}

func TestCalculateBackoff_CapsAtMaxDelay(t *testing.T) {
	cfg := baseConfig()
	cfg.BackoffOptions.Base = 1 * time.Hour
	cfg.BackoffOptions.Multiplier = 10
	cfg.BackoffOptions.MaxDelay = 2 * time.Hour
	d := &Dispatcher{config: cfg}

	for range 20 {
		got := d.calculateBackoff(5)
		assert.Equal(t, 2*time.Hour, got)
	}
}

func TestGenerateHMAC_MatchesStdlib(t *testing.T) {
	d := &Dispatcher{}
	secret := []byte("super-secret")
	body := []byte(`{"hello":"world"}`)

	got, err := d.generateHMAC(secret, body)
	require.NoError(t, err)

	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	want := hex.EncodeToString(mac.Sum(nil))

	assert.Equal(t, want, got)
}

func TestGetCircuitBreaker_SameIDReturnsSame(t *testing.T) {
	d := &Dispatcher{config: baseConfig()}
	a := d.getCircuitBreaker("wh_1")
	b := d.getCircuitBreaker("wh_1")
	assert.Same(t, a, b)
}

func TestGetCircuitBreaker_DifferentIDsReturnDifferent(t *testing.T) {
	d := &Dispatcher{config: baseConfig()}
	a := d.getCircuitBreaker("wh_1")
	b := d.getCircuitBreaker("wh_2")
	assert.NotSame(t, a, b)
}

func TestGetLimiter_SameIDReturnsSame(t *testing.T) {
	d := &Dispatcher{config: baseConfig()}
	a := d.getLimiter("wh_1")
	b := d.getLimiter("wh_1")
	assert.Same(t, a, b)
}

// dispatch() end-to-end

func TestDispatch_Success_Returns200(t *testing.T) {
	srv := newHookServer(http.StatusOK, nil)
	defer srv.Close()

	repo := mocks.NewMockRepository(t)
	repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(activeWebhook(srv.URL), nil)

	d := NewDispatcher(mocks.NewMockKafkaReader(t), repo, baseConfig())
	sc, err := d.dispatch(context.Background(), sampleEvent())

	require.NoError(t, err)
	require.NotNil(t, sc)
	assert.Equal(t, http.StatusOK, *sc)
}

func TestDispatch_4xxIsPermanent(t *testing.T) {
	for _, code := range []int{400, 403, 404, 422} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := newHookServer(code, nil)
			defer srv.Close()

			repo := mocks.NewMockRepository(t)
			repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(activeWebhook(srv.URL), nil)

			d := NewDispatcher(mocks.NewMockKafkaReader(t), repo, baseConfig())
			_, err := d.dispatch(context.Background(), sampleEvent())

			require.Error(t, err)
			assert.True(t, isPermanent(err), "code %d must yield permanent error", code)
		})
	}
}

func TestDispatch_429IsRetriable(t *testing.T) {
	srv := newHookServer(http.StatusTooManyRequests, nil)
	defer srv.Close()

	repo := mocks.NewMockRepository(t)
	repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(activeWebhook(srv.URL), nil)

	d := NewDispatcher(mocks.NewMockKafkaReader(t), repo, baseConfig())
	_, err := d.dispatch(context.Background(), sampleEvent())

	require.Error(t, err)
	assert.False(t, isPermanent(err), "429 must be retriable")
}

func TestDispatch_5xxIsRetriable(t *testing.T) {
	srv := newHookServer(http.StatusServiceUnavailable, nil)
	defer srv.Close()

	repo := mocks.NewMockRepository(t)
	repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(activeWebhook(srv.URL), nil)

	d := NewDispatcher(mocks.NewMockKafkaReader(t), repo, baseConfig())
	_, err := d.dispatch(context.Background(), sampleEvent())

	require.Error(t, err)
	assert.False(t, isPermanent(err), "5xx must be retriable")
}

func TestDispatch_WebhookNotFoundIsPermanent(t *testing.T) {
	repo := mocks.NewMockRepository(t)
	repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(domain.Webhook{}, mongo.ErrNoDocuments)

	d := NewDispatcher(mocks.NewMockKafkaReader(t), repo, baseConfig())
	_, err := d.dispatch(context.Background(), sampleEvent())

	require.Error(t, err)
	assert.True(t, isPermanent(err))
}

func TestDispatch_SendsHMACSignatureHeader(t *testing.T) {
	var captured capturedRequest
	srv := newHookServer(http.StatusOK, &captured)
	defer srv.Close()

	wh := activeWebhook(srv.URL)
	repo := mocks.NewMockRepository(t)
	repo.EXPECT().FindWebhook(mock.Anything, wh.ID).Return(wh, nil)

	d := NewDispatcher(mocks.NewMockKafkaReader(t), repo, baseConfig())
	_, err := d.dispatch(context.Background(), sampleEvent())
	require.NoError(t, err)

	gotSig := captured.headers.Get("X-Signature")
	require.NotEmpty(t, gotSig)

	mac := hmac.New(sha256.New, []byte(wh.Secret))
	_, _ = mac.Write(captured.body)
	wantSig := hex.EncodeToString(mac.Sum(nil))

	assert.Equal(t, wantSig, gotSig)
	assert.Equal(t, "application/json", captured.headers.Get("Content-Type"))
}

// worker()

func runWorkerOnce(t *testing.T, d *Dispatcher, msg kafka.Message, ev domain.Event) {
	t.Helper()
	jobs := make(chan job, 1)
	jobs <- job{msg: msg, event: ev}
	close(jobs)
	var wg sync.WaitGroup
	wg.Add(1)
	d.worker(context.Background(), jobs, &wg)
	wg.Wait()
}

func TestWorker_DeliveredOn2xx(t *testing.T) {
	srv := newHookServer(http.StatusOK, nil)
	defer srv.Close()

	repo := mocks.NewMockRepository(t)
	reader := mocks.NewMockKafkaReader(t)

	repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(activeWebhook(srv.URL), nil)
	repo.EXPECT().UpdateEventAttempt(mock.Anything, "ev_1", domain.EventStatusDelivered,
		(*time.Time)(nil), mock.Anything).Return(nil)
	reader.EXPECT().CommitMessages(mock.Anything, mock.Anything).Return(nil)

	d := NewDispatcher(reader, repo, baseConfig())
	runWorkerOnce(t, d, kafka.Message{Key: []byte("ev_1")}, sampleEvent())
}

func TestWorker_PermanentFailureMarksPoison(t *testing.T) {
	srv := newHookServer(http.StatusBadRequest, nil)
	defer srv.Close()

	repo := mocks.NewMockRepository(t)
	reader := mocks.NewMockKafkaReader(t)

	repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(activeWebhook(srv.URL), nil)
	repo.EXPECT().UpdateEventAttempt(mock.Anything, "ev_1", domain.EventStatusPoison,
		(*time.Time)(nil), mock.Anything).Return(nil)
	reader.EXPECT().CommitMessages(mock.Anything, mock.Anything).Return(nil)

	d := NewDispatcher(reader, repo, baseConfig())
	runWorkerOnce(t, d, kafka.Message{Key: []byte("ev_1")}, sampleEvent())
}

func TestWorker_RetriableFailureSetsNextRetryAt(t *testing.T) {
	srv := newHookServer(http.StatusServiceUnavailable, nil)
	defer srv.Close()

	repo := mocks.NewMockRepository(t)
	reader := mocks.NewMockKafkaReader(t)

	repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(activeWebhook(srv.URL), nil)

	var gotStatus domain.EventStatus
	var gotNextRetry *time.Time
	repo.EXPECT().UpdateEventAttempt(mock.Anything, "ev_1", mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, status domain.EventStatus, nextRetryAt *time.Time, _ domain.DeliveryAttempt) {
			gotStatus = status
			gotNextRetry = nextRetryAt
		}).Return(nil)
	reader.EXPECT().CommitMessages(mock.Anything, mock.Anything).Return(nil)

	d := NewDispatcher(reader, repo, baseConfig())
	runWorkerOnce(t, d, kafka.Message{Key: []byte("ev_1")}, sampleEvent())

	assert.Equal(t, domain.EventStatusFailed, gotStatus)
	require.NotNil(t, gotNextRetry, "retriable failure must set next_retry_at")
	assert.True(t, gotNextRetry.After(time.Now()))
}

func TestWorker_DeadWhenBackoffPastRetentionWindow(t *testing.T) {
	srv := newHookServer(http.StatusServiceUnavailable, nil)
	defer srv.Close()

	repo := mocks.NewMockRepository(t)
	reader := mocks.NewMockKafkaReader(t)

	ev := sampleEvent()
	ev.CreatedAt = time.Now().UTC().Add(-200 * time.Hour) // much older than 72h retention

	repo.EXPECT().FindWebhook(mock.Anything, "wh_1").Return(activeWebhook(srv.URL), nil)
	repo.EXPECT().UpdateEventAttempt(mock.Anything, "ev_1", domain.EventStatusDead,
		(*time.Time)(nil), mock.Anything).Return(nil)
	reader.EXPECT().CommitMessages(mock.Anything, mock.Anything).Return(nil)

	d := NewDispatcher(reader, repo, baseConfig())
	runWorkerOnce(t, d, kafka.Message{Key: []byte("ev_1")}, ev)
}

// watchEvents poison-on-bad-json path

func TestWatchEvents_BadJSON_MarksPoisonAndCommits(t *testing.T) {
	repo := mocks.NewMockRepository(t)
	reader := mocks.NewMockKafkaReader(t)

	badMsg := kafka.Message{Key: []byte("ev_bad"), Value: []byte(`not valid json {`)}

	ctx, cancel := context.WithCancel(context.Background())

	// 1st fetch returns the bad message; 2nd fetch cancels the ctx and returns an error,
	// forcing watchEvents to exit on the next iteration's ctx check. No goroutines needed.
	reader.EXPECT().FetchMessage(mock.Anything).Return(badMsg, nil).Once()
	reader.EXPECT().FetchMessage(mock.Anything).
		RunAndReturn(func(_ context.Context) (kafka.Message, error) {
			cancel()
			return kafka.Message{}, context.Canceled
		}).Once()

	repo.EXPECT().MarkEventPoison(mock.Anything, "ev_bad", mock.Anything).Return(nil)
	reader.EXPECT().CommitMessages(mock.Anything, badMsg).Return(nil)

	d := NewDispatcher(reader, repo, baseConfig())
	d.watchEvents(ctx, make(chan job, 1))
}

// helpers

func pow(x, n float64) float64 {
	result := 1.0
	for range int(n) {
		result *= x
	}
	return result
}
