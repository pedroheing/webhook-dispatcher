package schedule

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"webhook-dispatcher/internal/pkg/domain"

	"github.com/segmentio/kafka-go"
)

type Repository interface {
	FetchDueRetries(ctx context.Context, limit int64, now time.Time) ([]domain.Event, error)
	MarkPending(ctx context.Context, ids []string) error
}

type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Scheduler struct {
	writer     KafkaWriter
	repository Repository
	config     Config
}

type Config struct {
	DbPullRate         time.Duration
	DbBatchSize        int64
	PendingEventsTopic string
}

func NewScheduler(writer KafkaWriter, repository Repository, config Config) *Scheduler {
	return &Scheduler{writer: writer, repository: repository, config: config}
}

func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(s.config.DbPullRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.processRetries(ctx)
		}
	}
}

func (s *Scheduler) processRetries(ctx context.Context) {
	events, err := s.repository.FetchDueRetries(ctx, s.config.DbBatchSize, time.Now())
	if err != nil {
		log.Printf("error getting events: %v", err)
		return
	}

	ids, err := s.publishOnBroker(ctx, events)
	if err != nil {
		log.Printf("error republishing: %v", err)
		return
	}

	if len(ids) > 0 {
		if err := s.repository.MarkPending(ctx, ids); err != nil {
			log.Printf("error marking pending: %v", err)
		}
	}
}

func (s *Scheduler) publishOnBroker(ctx context.Context, events []domain.Event) ([]string, error) {
	var messages []kafka.Message
	var ids []string
	for _, e := range events {
		e.Status = domain.EventStatusPending
		payload, _ := json.Marshal(e)
		messages = append(messages, kafka.Message{
			Topic: s.config.PendingEventsTopic,
			Key:   []byte(e.ID),
			Value: payload,
		})
		ids = append(ids, e.ID)
	}

	if len(messages) == 0 {
		return ids, nil
	}

	if err := s.writer.WriteMessages(ctx, messages...); err != nil {
		return nil, err
	}
	return ids, nil
}
