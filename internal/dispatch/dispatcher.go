package dispatch

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
	"webhook-dispatcher/internal/pkg/domain"

	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker/v2"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/time/rate"
)

type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Repository interface {
	FindWebhook(ctx context.Context, id string) (domain.Webhook, error)
	UpdateEventAttempt(ctx context.Context, eventID string, status domain.EventStatus, nextRetryAt *time.Time, attempt domain.DeliveryAttempt) error
	MarkEventPoison(ctx context.Context, eventID string, attempt domain.DeliveryAttempt) error
}

type Dispatcher struct {
	reader     KafkaReader
	repository Repository
	httpClient *http.Client
	breakers   sync.Map
	limiters   sync.Map
	config     Config
}

type job struct {
	msg   kafka.Message
	event domain.Event
}

type Config struct {
	Workers               uint32
	BufferSize            uint32
	RetentionWindow       time.Duration
	CircuitBreakerOptions CircuitBreakerOptions
	LimiterOptions        LimiterOptions
	BackoffOptions        BackoffOptions
	HttpOptions           HttpOptions
}

type CircuitBreakerOptions struct {
	// How many requests go throw while half-open
	MaxRequests uint32
	// How long until ConsecutiveFailures is reset while in closed state
	Interval time.Duration
	// How long it lasts on open before going to half-open
	Timeout time.Duration
	// How many failures before circuit open
	FailuresBeforeOpen uint32
}

type LimiterOptions struct {
	RefillRate rate.Limit
	BucketSize int
}

type BackoffOptions struct {
	// Base time that will be used to calculate the backoff
	Base time.Duration
	// Max amout of time for the backoff
	MaxDelay time.Duration
	// Base of the backoffer power calculation.
	// multiplier ^ deliveryAttempts
	Multiplier float64
}

type HttpOptions struct {
	Timeout time.Duration
}

type webhookPayload struct {
	ID        string          `json:"id"`
	EventType string          `json:"event_type"`
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"created_at"`
}

func NewDispatcher(reader KafkaReader, repository Repository, config Config) *Dispatcher {
	return &Dispatcher{
		reader:     reader,
		repository: repository,
		httpClient: &http.Client{
			Timeout: config.HttpOptions.Timeout,
		},
		config: config,
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	jobs := make(chan job, d.config.BufferSize)
	var wg sync.WaitGroup
	for range d.config.Workers {
		wg.Add(1)
		go d.worker(ctx, jobs, &wg)
	}
	d.watchEvents(ctx, jobs)
	// it gets here when ctx is canceled, like when the program is closed
	close(jobs)
	wg.Wait()
}

func (d *Dispatcher) watchEvents(ctx context.Context, jobs chan<- job) {
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := d.reader.FetchMessage(ctx)
		if err != nil {
			log.Println(err)
			continue
		}
		var event domain.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("unmarshal event failed: %v", err)
			if err := d.registerEventAsPoison(ctx, msg, err); err != nil {
				log.Printf("mark poison failed for key %q: %v", string(msg.Key), err)
				continue
			}
			if commitErr := d.reader.CommitMessages(ctx, msg); commitErr != nil {
				log.Printf("commit failed: %v", commitErr)
			}
			continue
		}
		jobs <- job{msg: msg, event: event}
	}
}

func (d *Dispatcher) worker(ctx context.Context, jobs <-chan job, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range jobs {
		now := time.Now().UTC()
		statusCode, err := d.dispatch(ctx, j.event)

		attempt := domain.DeliveryAttempt{
			At:         now,
			HTTPStatus: statusCode,
		}

		if err == nil {
			attempt.Status = domain.EventStatusDelivered
		} else if isPermanent(err) {
			attempt.Status = domain.EventStatusPoison
			attempt.Error = err.Error()
		} else {
			attempt.Status = domain.EventStatusFailed
			attempt.Error = err.Error()
		}

		eventStatus := attempt.Status
		var nextRetryAt *time.Time
		if attempt.Status == domain.EventStatusFailed {
			retryDate := now.Add(d.calculateBackoff(j.event.AttemptNumber + 1))
			cutoffDate := j.event.CreatedAt.Add(d.config.RetentionWindow)
			if retryDate.After(cutoffDate) {
				eventStatus = domain.EventStatusDead
			} else {
				nextRetryAt = &retryDate
			}
		}

		if err := d.repository.UpdateEventAttempt(ctx, j.event.ID, eventStatus, nextRetryAt, attempt); err != nil {
			log.Printf("error saving event dispatch data %v", err)
			continue
		}
		if commitErr := d.reader.CommitMessages(ctx, j.msg); commitErr != nil {
			log.Printf("commit failed: %v", commitErr)
		}
	}
}

func (d *Dispatcher) registerEventAsPoison(ctx context.Context, msg kafka.Message, cause error) error {
	eventID := string(msg.Key)
	if eventID == "" {
		return fmt.Errorf("missing message key")
	}
	attempt := domain.DeliveryAttempt{
		At:     time.Now().UTC(),
		Status: domain.EventStatusPoison,
		Error:  cause.Error(),
	}
	return d.repository.MarkEventPoison(ctx, eventID, attempt)
}

func (d *Dispatcher) dispatch(ctx context.Context, event domain.Event) (*int, error) {
	webhook, err := d.repository.FindWebhook(ctx, event.WebhookID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, NewPermanentError(fmt.Errorf("webhook not found: %s", event.WebhookID))
		}
		return nil, err
	}
	payload, err := json.Marshal(webhookPayload{
		ID:        event.ID,
		EventType: event.EventType,
		Data:      event.Data,
		CreatedAt: event.CreatedAt,
	})
	if err != nil {
		return nil, NewPermanentError(err)
	}

	signature, err := d.generateHMAC([]byte(webhook.Secret), payload)
	if err != nil {
		return nil, err
	}

	if err := d.getLimiter(event.WebhookID).Wait(ctx); err != nil {
		return nil, err
	}

	statusCode, err := d.getCircuitBreaker(event.WebhookID).Execute(func() (*int, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", webhook.Url, bytes.NewBuffer(payload))
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Signature", signature)

		resp, err := d.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
			return &resp.StatusCode, NewPermanentError(fmt.Errorf("client error: %d", resp.StatusCode))
		}

		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return &resp.StatusCode, fmt.Errorf("client error: %d", resp.StatusCode)
		}

		_, err = io.Copy(io.Discard, resp.Body)
		return &resp.StatusCode, err
	})

	return statusCode, err
}

func (d *Dispatcher) getCircuitBreaker(id string) *gobreaker.CircuitBreaker[*int] {
	if val, ok := d.breakers.Load(id); ok {
		return val.(*gobreaker.CircuitBreaker[*int])
	}
	cb := gobreaker.NewCircuitBreaker[*int](gobreaker.Settings{
		Name:        id,
		MaxRequests: d.config.CircuitBreakerOptions.MaxRequests,
		Interval:    d.config.CircuitBreakerOptions.Interval,
		Timeout:     d.config.CircuitBreakerOptions.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= d.config.CircuitBreakerOptions.FailuresBeforeOpen
		},
	})
	actual, _ := d.breakers.LoadOrStore(id, cb)
	return actual.(*gobreaker.CircuitBreaker[*int])
}

func (d *Dispatcher) getLimiter(id string) *rate.Limiter {
	if val, ok := d.limiters.Load(id); ok {
		return val.(*rate.Limiter)
	}
	l := rate.NewLimiter(d.config.LimiterOptions.RefillRate, d.config.LimiterOptions.BucketSize)
	actual, _ := d.limiters.LoadOrStore(id, l)
	return actual.(*rate.Limiter)
}

func (d *Dispatcher) generateHMAC(secret, body []byte) (string, error) {
	h := hmac.New(sha256.New, secret)
	_, err := h.Write(body)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (d *Dispatcher) calculateBackoff(attempt int) time.Duration {
	delay := float64(d.config.BackoffOptions.Base) * math.Pow(d.config.BackoffOptions.Multiplier, float64(attempt))

	jitter := (rand.Float64() * 0.2) - 0.1 // -10% to +10%
	delay += delay * jitter

	duration := time.Duration(delay)
	if duration > d.config.BackoffOptions.MaxDelay {
		return d.config.BackoffOptions.MaxDelay
	}
	return duration
}
