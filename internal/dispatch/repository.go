package dispatch

import (
	"context"
	"time"
	"webhook-dispatcher/internal/pkg/domain"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type MongoRepository struct {
	db *mongo.Database
}

func NewMongoRepository(db *mongo.Database) *MongoRepository {
	return &MongoRepository{db: db}
}

func (s *MongoRepository) FindWebhook(ctx context.Context, id string) (domain.Webhook, error) {
	var wh domain.Webhook
	err := s.db.Collection(domain.CollectionWebhooks).FindOne(ctx, bson.M{"_id": id}).Decode(&wh)
	if err != nil {
		return domain.Webhook{}, err
	}
	return wh, nil
}

func (s *MongoRepository) UpdateEventAttempt(ctx context.Context, eventID string, status domain.EventStatus, nextRetryAt *time.Time, attempt domain.DeliveryAttempt) error {
	_, err := s.db.Collection(domain.CollectionEvents).UpdateByID(ctx, eventID, bson.M{
		"$set": bson.M{
			"status":        status,
			"next_retry_at": nextRetryAt,
		},
		"$inc":  bson.M{"attempt_number": 1},
		"$push": bson.M{"attempts": attempt},
	})
	return err
}

func (s *MongoRepository) MarkEventPoison(ctx context.Context, eventID string, attempt domain.DeliveryAttempt) error {
	_, err := s.db.Collection(domain.CollectionEvents).UpdateByID(ctx, eventID, bson.M{
		"$set": bson.M{
			"status": domain.EventStatusPoison,
		},
		"$push": bson.M{
			"attempts": attempt,
		},
	})
	return err
}
