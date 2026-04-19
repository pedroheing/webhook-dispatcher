package schedule

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"webhook-dispatcher/internal/pkg/domain"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Scheduler struct {
	writer *kafka.Writer
	db     *mongo.Database
	config Config
}

type Config struct {
	DbPullRate         time.Duration
	DbBatchSize        int64
	PendingEventsTopic string
}

func NewScheduler(writer *kafka.Writer, db *mongo.Database, config Config) *Scheduler {
	return &Scheduler{writer: writer, db: db, config: config}
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
	findOptions := options.Find().
		SetLimit(s.config.DbBatchSize).
		SetSort(bson.M{"next_retry_at": 1})
	cursor, err := s.db.Collection(domain.CollectionEvents).Find(ctx, bson.M{
		"status":        domain.EventStatusFailed,
		"next_retry_at": bson.M{"$lte": time.Now()},
	}, findOptions)
	if err != nil {
		log.Printf("error getting events: %v", err)
		return
	}
	var events []domain.Event
	if err := cursor.All(ctx, &events); err != nil {
		log.Printf("error parsing events: %v", err)
		return
	}

	ids, err := s.publishOnBroker(ctx, events)
	if err != nil {
		log.Printf("error republishing: %v", err)
		return
	}

	if len(ids) > 0 {
		s.db.Collection(domain.CollectionEvents).UpdateMany(ctx, bson.M{
			"_id": bson.M{"$in": ids},
		}, bson.M{
			"$set": bson.M{"status": domain.EventStatusPending},
		})
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
