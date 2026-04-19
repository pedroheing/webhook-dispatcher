package webhook

import (
	"context"
	"webhook-dispatcher/internal/pkg/domain"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoRepository struct {
	db *mongo.Database
}

type WebhookPatch struct {
	Url           *string  `bson:"url,omitempty"`
	EnabledEvents []string `bson:"enabled_events,omitempty"`
	Active        *bool    `bson:"active,omitempty"`
}

func NewMongoRepository(db *mongo.Database) *MongoRepository {
	return &MongoRepository{db: db}
}

func (r *MongoRepository) collWebhooks() *mongo.Collection {
	return r.db.Collection(domain.CollectionWebhooks)
}

func (r *MongoRepository) collEvents() *mongo.Collection {
	return r.db.Collection(domain.CollectionEvents)
}

func (r *MongoRepository) Create(ctx context.Context, wh domain.Webhook) error {
	_, err := r.collWebhooks().InsertOne(ctx, wh)
	return err
}

func (r *MongoRepository) CreateEvent(ctx context.Context, ev domain.Event) error {
	_, err := r.collEvents().InsertOne(ctx, ev)
	return err
}

func (r *MongoRepository) Update(ctx context.Context, id string, wh WebhookPatch) (domain.Webhook, error) {
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var updated domain.Webhook
	err := r.collWebhooks().FindOneAndUpdate(ctx, bson.M{"_id": id}, bson.M{
		"$set": wh,
	}, opts).Decode(&updated)
	if err != nil {
		return domain.Webhook{}, err
	}
	return updated, nil
}

func (r *MongoRepository) Delete(ctx context.Context, id string) error {
	res, err := r.collWebhooks().DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return mongo.ErrNoDocuments
	}
	return nil
}

func (r *MongoRepository) Get(ctx context.Context, id string) (domain.Webhook, error) {
	var wh domain.Webhook
	err := r.collWebhooks().FindOne(ctx, bson.M{"_id": id}).Decode(&wh)
	if err != nil {
		return domain.Webhook{}, err
	}
	return wh, nil
}

func (r *MongoRepository) List(ctx context.Context) ([]domain.Webhook, error) {
	cursor, err := r.collWebhooks().Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	var whs []domain.Webhook
	if err := cursor.All(ctx, &whs); err != nil {
		return nil, err
	}
	return whs, nil
}
