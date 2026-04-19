package main

import (
	"context"
	"log"
	"time"
	"webhook-dispatcher/internal/dispatch"
	"webhook-dispatcher/internal/pkg/env"
	"webhook-dispatcher/internal/pkg/kafka"
	"webhook-dispatcher/internal/pkg/mongo"

	mongoDriver "go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/time/rate"
)

type Config struct {
	APIPort    int              `env:"API_PORT_DISPATCHER,required"`
	Kafka      KafkaConfig      `envPrefix:"KAFKA_"`
	Mongo      MongoConfig      `envPrefix:"MONGO_"`
	Dispatcher DispatcherConfig `envPrefix:"DISPATCHER_"`
}

type KafkaConfig struct {
	Brokers       []string `env:"BROKERS,required" envSeparator:","`
	Topic         string   `env:"TOPIC,required"`
	ConsumerGroup string   `env:"CONSUMER_GROUP,required"`
}

type MongoConfig struct {
	URI      string `env:"URI,required"`
	Database string `env:"DATABASE,required"`
}

type DispatcherConfig struct {
	Workers         uint32        `env:"WORKERS" envDefault:"100"`
	BufferSize      uint32        `env:"BUFFER_SIZE" envDefault:"1000"`
	RetentionWindow time.Duration `env:"RETENTION_WINDOW" envDefault:"72h"`

	CBMaxRequests        uint32        `env:"CB_MAX_REQUESTS" envDefault:"1"`
	CBInterval           time.Duration `env:"CB_INTERVAL" envDefault:"60s"`
	CBTimeout            time.Duration `env:"CB_TIMEOUT" envDefault:"60s"`
	CBFailuresBeforeOpen uint32        `env:"CB_FAILURES_BEFORE_OPEN" envDefault:"5"`

	LimiterRefillRate float64 `env:"LIMITER_REFILL_RATE" envDefault:"2"`
	LimiterBucketSize int     `env:"LIMITER_BUCKET_SIZE" envDefault:"20"`

	BackoffBase       time.Duration `env:"BACKOFF_BASE" envDefault:"30s"`
	BackoffMaxDelay   time.Duration `env:"BACKOFF_MAX_DELAY" envDefault:"6h"`
	BackoffMultiplier float64       `env:"BACKOFF_MULTIPLIER" envDefault:"2"`

	HttpTimeout time.Duration `env:"HTTP_TIMEOUT" envDefault:"30s"`
}

func main() {
	config, err := env.Load[Config]()
	if err != nil {
		panic(err)
	}
	reader := kafka.NewReader(config.Kafka.Brokers, config.Kafka.Topic, config.Kafka.ConsumerGroup)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("error on closing kafka reader: %v", err)
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
	dispatcher := dispatch.NewDispatcher(reader, mongoClient.Database(config.Mongo.Database), buildDispatchConfig(config.Dispatcher))
	dispatcher.Start(context.Background())
}

func buildDispatchConfig(c DispatcherConfig) dispatch.Config {
	return dispatch.Config{
		Workers:         c.Workers,
		BufferSize:      c.BufferSize,
		RetentionWindow: c.RetentionWindow,
		CircuitBreakerOptions: dispatch.CircuitBreakerOptions{
			MaxRequests:        c.CBMaxRequests,
			Interval:           c.CBInterval,
			Timeout:            c.CBTimeout,
			FailuresBeforeOpen: c.CBFailuresBeforeOpen,
		},
		LimiterOptions: dispatch.LimiterOptions{
			RefillRate: rate.Limit(c.LimiterRefillRate),
			BucketSize: c.LimiterBucketSize,
		},
		BackoffOptions: dispatch.BackoffOptions{
			Base:       c.BackoffBase,
			MaxDelay:   c.BackoffMaxDelay,
			Multiplier: c.BackoffMultiplier,
		},
		HttpOptions: dispatch.HttpOptions{
			Timeout: c.HttpTimeout,
		},
	}
}

func disconnectMongo(client *mongoDriver.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		log.Printf("error on mongo disconnect: %v", err)
	}
}
