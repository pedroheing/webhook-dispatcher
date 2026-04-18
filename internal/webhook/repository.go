package webhook

import (
	"context"
	"webhook-dispatcher/internal/pkg/domain"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Repository struct {
	db *mongo.Database
}

type WebhookPatch struct {
	Url           *string  `bson:"url,omitempty"`
	EnabledEvents []string `bson:"enabled_events,omitempty"`
	Active        *bool    `bson:"active,omitempty"`
}

func NewRepository(db *mongo.Database) *Repository {
	return &Repository{db: db}
}

func (r *Repository) coll() *mongo.Collection {
	return r.db.Collection("webhooks")
}

func (r *Repository) Create(ctx context.Context, wh domain.Webhook) error {
	_, err := r.coll().InsertOne(ctx, wh)
	return err
}

func (r *Repository) Update(ctx context.Context, id string, wh WebhookPatch) (domain.Webhook, error) {
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var updated domain.Webhook
	err := r.coll().FindOneAndUpdate(ctx, bson.M{"_id": id}, bson.M{
		"$set": wh,
	}, opts).Decode(&updated)
	if err != nil {
		return domain.Webhook{}, err
	}
	return updated, nil
}

func (r *Repository) Delete(ctx context.Context, id string) error {
	res, err := r.coll().DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return mongo.ErrNoDocuments
	}
	return nil
}

func (r *Repository) Get(ctx context.Context, id string) (domain.Webhook, error) {
	var wh domain.Webhook
	err := r.coll().FindOne(ctx, bson.M{"_id": id}).Decode(wh)
	if err != nil {
		return domain.Webhook{}, err
	}
	return wh, nil
}

func (r *Repository) List(ctx context.Context) ([]domain.Webhook, error) {
	cursor, err := r.coll().Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	var whs []domain.Webhook
	if err := cursor.All(ctx, &whs); err != nil {
		return nil, err
	}
	return whs, nil
}
