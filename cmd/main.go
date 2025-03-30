package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/joho/godotenv"
	"github.com/melnikdev/go-logs-consumer/internal/config"
	"github.com/melnikdev/go-logs-consumer/internal/service"
	"github.com/segmentio/kafka-go"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Fatal("Error loading .env file")
	}

	config := config.NewConfig()

	db, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.ClickHouse.Addr},
		Auth: clickhouse.Auth{
			Username: config.ClickHouse.Username,
			Password: config.ClickHouse.Password,
			Database: config.ClickHouse.Database,
		},
	})

	if err != nil {
		log.Fatal("Error while connecting to ClickHouse:", err)
	}
	defer db.Close()

	// ctx := context.Background()

	// createTableQuery := `
	// CREATE TABLE IF NOT EXISTS logs (
	// 	service String,
	// 	level String,
	// 	message String,
	// 	timestamp DateTime
	// ) ENGINE = MergeTree()
	// ORDER BY timestamp;
	// `

	// err = conn.Exec(ctx, createTableQuery)
	// if err != nil {
	// 	log.Fatalf("Error creating table: %v", err)
	// }

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.Kafka.Broker},
		Topic:   config.Kafka.Topic,
		GroupID: config.Kafka.GroupID,
	})

	fmt.Println("🚀 Kafka Consumer started")
	var wg sync.WaitGroup
	worker := service.NewWorker(db, config)

	logChannel := make(chan kafka.Message, 1000)

	for i := 0; i < config.Worker.Count; i++ {
		wg.Add(1)
		go worker.SaveBatchLogMessages(logChannel, &wg)
	}

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error read Kafka: %v", err)
			continue
		}

		logChannel <- msg
	}
}
