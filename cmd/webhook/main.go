package main

import (
	"context"
	"log"
	"time"
	"webhook-dispatcher/internal/pkg/mongo"
	"webhook-dispatcher/internal/webhook"

	mongoDriver "go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.New()
	mongoClient, err := mongo.Connect(context.Background(), "mongodb://admin:password@localhost:27017/?authSource=admin")
	if err != nil {
		panic(err)
	}
	defer disconnectMongo(mongoClient)
	repository := webhook.NewRepository(mongoClient.Database("webhook-dispatcher"))
	handler := webhook.NewHandler(repository)
	v1 := router.Group("/v1")
	handler.RegisterRoutes(v1)
	if err := router.Run(":8084"); err != nil {
		panic(err)
	}
}

func disconnectMongo(client *mongoDriver.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		log.Printf("error on mongo disconnect: %v", err)
	}
}
