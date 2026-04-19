package schedule

import (
	"context"
	"time"
	"webhook-dispatcher/internal/pkg/domain"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoRepository struct {
	db *mongo.Database
}

func NewMongoRepository(db *mongo.Database) *MongoRepository {
	return &MongoRepository{db: db}
}

func (s *MongoRepository) FetchDueRetries(ctx context.Context, limit int64, now time.Time) ([]domain.Event, error) {
	findOptions := options.Find().
		SetLimit(limit).
		SetSort(bson.M{"next_retry_at": 1})
	cursor, err := s.db.Collection(domain.CollectionEvents).Find(ctx, bson.M{
		"status":        domain.EventStatusFailed,
		"next_retry_at": bson.M{"$lte": now},
	}, findOptions)
	if err != nil {
		return nil, err
	}
	var events []domain.Event
	if err := cursor.All(ctx, &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *MongoRepository) MarkPending(ctx context.Context, ids []string) error {
	_, err := s.db.Collection(domain.CollectionEvents).UpdateMany(ctx, bson.M{
		"_id": bson.M{"$in": ids},
	}, bson.M{
		"$set": bson.M{"status": domain.EventStatusPending},
	})
	return err
}
