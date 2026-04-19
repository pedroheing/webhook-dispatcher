package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"webhook-dispatcher/internal/pkg/env"
	"webhook-dispatcher/internal/pkg/kafka"
	"webhook-dispatcher/internal/pkg/mongo"
	"webhook-dispatcher/internal/webhook"

	"github.com/gin-gonic/gin"
	mongoDriver "go.mongodb.org/mongo-driver/v2/mongo"
)

type Config struct {
	APIPort int         `env:"API_PORT_WEBHOOK,required"`
	Kafka   KafkaConfig `envPrefix:"KAFKA_"`
	Mongo   MongoConfig `envPrefix:"MONGO_"`
}

type KafkaConfig struct {
	Brokers []string `env:"BROKERS,required" envSeparator:","`
	Topic   string   `env:"TOPIC,required"`
}

type MongoConfig struct {
	URI      string `env:"URI,required"`
	Database string `env:"DATABASE,required"`
}

func main() {
	config, err := env.Load[Config]()
	if err != nil {
		panic(err)
	}
	writter := kafka.NewWriter(config.Kafka.Brokers)
	defer func() {
		if err := writter.Close(); err != nil {
			log.Printf("error on closing kafka writter: %v", err)
		}
	}()
	// it only creates on one, kafka replicates
	if err := kafka.CreateTopics(config.Kafka.Brokers[0], config.Kafka.Topic); err != nil {
		log.Printf("create topics: %v", err)
	}

	mongoClient, err := mongo.Connect(context.Background(), config.Mongo.URI)
	if err != nil {
		panic(err)
	}
	defer disconnectMongo(mongoClient)

	webhookConfig := webhook.Config{
		PendingEventsTopic: config.Kafka.Topic,
	}

	repository := webhook.NewMongoRepository(mongoClient.Database(config.Mongo.Database))
	webhookHandler := webhook.NewHandler(writter, repository, webhookConfig)

	r := gin.Default()
	group := r.Group("v1")
	webhookHandler.RegisterRoutes(group)

	log.Printf("Starting server on port %d...", config.APIPort)
	if err := r.Run(fmt.Sprintf(":%d", config.APIPort)); err != nil {
		log.Fatalf("Error: %v", err)
	}
	log.Println("Http server initiated")
}

func disconnectMongo(client *mongoDriver.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		log.Printf("error on mongo disconnect: %v", err)
	}
}
