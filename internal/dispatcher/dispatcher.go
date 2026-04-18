package dispatcher

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
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/time/rate"
)

type Dispatcher struct {
	reader     *kafka.Reader
	db         *mongo.Database
	httpClient *http.Client
	breakers   sync.Map
	limiters   sync.Map
}

type job struct {
	msg   kafka.Message
	event domain.Event
}

const MaxRetention = 72 * time.Hour // 3 days

func New(reader *kafka.Reader, db *mongo.Database) *Dispatcher {
	return &Dispatcher{
		reader: reader,
		db:     db,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *Dispatcher) Start(ctx context.Context, numWorkers, jobsBufferSize int) {
	jobs := make(chan job, jobsBufferSize)
	var wg sync.WaitGroup
	for range numWorkers {
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
			log.Println(err)
			d.reader.CommitMessages(ctx, msg)
			continue
		}
		jobs <- job{msg: msg, event: event}
	}
}

func (d *Dispatcher) worker(ctx context.Context, jobs <-chan job, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range jobs {
		now := time.Now().UTC()

		statusCode, err := d.dispatch(ctx, j)

		attempt := domain.DeliveryAttempt{
			At:         now,
			HTTPStatus: statusCode,
		}

		if err == nil {
			attempt.Status = "delivered"
		} else if isPermanent(err) {
			attempt.Status = "poison"
			attempt.Error = err.Error()
		} else {
			attempt.Status = "failed"
			attempt.Error = err.Error()
		}

		attemptNumber := j.event.AttemptNumber + 1
		eventStatus := attempt.Status
		var nexRetryAt time.Time

		if eventStatus == "failed" {
			nexRetryAt = now.Add(calculateBackoff(attemptNumber))
			cutoffDate := j.event.CreatedAt.Add(MaxRetention)
			if nexRetryAt.After(cutoffDate) {
				eventStatus = "dead"
			}
		}

		_, err = d.db.Collection("events").UpdateByID(ctx, j.event.ID, bson.M{
			"$set": bson.M{
				"status":         eventStatus,
				"attempt_number": attemptNumber,
				"next_retry_at":  nexRetryAt,
			},
			"$push": bson.M{
				"attempts": attempt,
			},
		})
		if err != nil {
			log.Printf("error saving event dispatch data %v", err)
			continue
		}
		if commitErr := d.reader.CommitMessages(ctx, j.msg); commitErr != nil {
			log.Printf("commit failed: %v", commitErr)
		}
	}
}

func (d *Dispatcher) dispatch(ctx context.Context, job job) (*int, error) {
	var webhook domain.Webhook
	err := d.db.Collection("webhooks").FindOne(ctx, bson.M{"_id": job.event.WebhookID}).Decode(&webhook)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, NewPermanentError(fmt.Errorf("webhook not found: %s", job.event.WebhookID))
		}
		return nil, err
	}
	payload, err := json.Marshal(job.event)
	if err != nil {
		return nil, NewPermanentError(err)
	}

	signature, err := d.generateHMAC([]byte(webhook.Secret), payload)
	if err != nil {
		return nil, err
	}

	if err := d.getLimiter(job.event.WebhookID).Wait(ctx); err != nil {
		return nil, err
	}

	statusCode, err := d.getCircuitBreaker(job.event.WebhookID).Execute(func() (*int, error) {
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
			return nil, NewPermanentError(fmt.Errorf("client error: %d", resp.StatusCode))
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
		Name: id,
		// how many requests go throw while half-open
		MaxRequests: 1,
		// how long until ConsecutiveFailures is reset while in closed state
		Interval: 30 * time.Second,
		// how long it lasts on open before going to half-open
		Timeout: 60 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > 5
		},
	})
	actual, _ := d.breakers.LoadOrStore(id, cb)
	return actual.(*gobreaker.CircuitBreaker[*int])
}

func (d *Dispatcher) getLimiter(id string) *rate.Limiter {
	if val, ok := d.limiters.Load(id); ok {
		return val.(*rate.Limiter)
	}
	l := rate.NewLimiter(rate.Limit(2), 20)
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

func calculateBackoff(attempt int) time.Duration {
	base := 30 * time.Second
	maxDelay := 6 * time.Hour
	multiplier := 2.0

	delay := float64(base) * math.Pow(multiplier, float64(attempt))

	jitter := (rand.Float64() * 0.2) - 0.1 // -10% to +10%
	delay += delay * jitter

	duration := time.Duration(delay)
	if duration > maxDelay {
		return maxDelay
	}
	return duration
}
