package main

import (
	"context"
	"log"
	"os"
	"time"
	"webhook-dispatcher/internal/dispatcher"
	"webhook-dispatcher/internal/pkg/kafka"
	"webhook-dispatcher/internal/pkg/mongo"

	"github.com/joho/godotenv"
	mongoDriver "go.mongodb.org/mongo-driver/v2/mongo"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println(".env not found")
	}
	brokerURL := os.Getenv("KAFKA_BROKER_URL")
	if brokerURL == "" {
		panic("KAFKA_BROKER_URL not found")
	}
	reader := kafka.NewReader([]string{brokerURL}, "events.pending", "webhook-dispatcher")
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("error on closing kafka reader: %v", err)
		}
	}()
	if err := kafka.CreateTopics(brokerURL, "events.pending"); err != nil {
		log.Printf("create topics: %v", err)
	}
	mongoClient, err := mongo.Connect(context.Background(), "mongodb://admin:password@localhost:27017/?authSource=admin")
	if err != nil {
		panic(err)
	}
	defer disconnectMongo(mongoClient)
	dispatcher := dispatcher.New(reader, mongoClient.Database("webhook-dispatcher"))
	dispatcher.Start(context.Background(), 100, 1000)
}

func disconnectMongo(client *mongoDriver.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		log.Printf("error on mongo disconnect: %v", err)
	}
}
