package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"webhook-dispatcher/internal/pkg/env"
	"webhook-dispatcher/internal/pkg/kafka"
	"webhook-dispatcher/internal/pkg/mongo"
	"webhook-dispatcher/internal/schedule"

	mongoDriver "go.mongodb.org/mongo-driver/v2/mongo"
)

type Config struct {
	Kafka     KafkaConfig     `envPrefix:"KAFKA_"`
	Mongo     MongoConfig     `envPrefix:"MONGO_"`
	Scheduler SchedulerConfig `envPrefix:"SCHEDULER_"`
}

type KafkaConfig struct {
	Brokers []string `env:"BROKERS,required" envSeparator:","`
	Topic   string   `env:"TOPIC,required"`
}

type MongoConfig struct {
	URI      string `env:"URI,required"`
	Database string `env:"DATABASE,required"`
}

type SchedulerConfig struct {
	DbPullRate  time.Duration `env:"DB_PULL_RATE" envDefault:"5s"`
	DbBatchSize int64         `env:"DB_BATCH_SIZE" envDefault:"1000"`
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
	schedulerConfig := schedule.Config{
		DbPullRate:         config.Scheduler.DbPullRate,
		PendingEventsTopic: config.Kafka.Topic,
		DbBatchSize:        config.Scheduler.DbBatchSize,
	}
	scheduler := schedule.NewScheduler(writter, mongoClient.Database(config.Mongo.Database), schedulerConfig)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	scheduler.Start(ctx)
}

func disconnectMongo(client *mongoDriver.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		log.Printf("error on mongo disconnect: %v", err)
	}
}
