package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/melnikdev/go-logs-customer/internal/config"
	"github.com/segmentio/kafka-go"
)

type LogMessage struct {
	Service   string `json:"service"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

type WorkerI interface {
	SaveBatchLogMessages(logChannel <-chan kafka.Message, wg *sync.WaitGroup)
}

type worker struct {
	db     clickhouse.Conn
	config *config.Config
}

func NewWorker(db clickhouse.Conn, config *config.Config) WorkerI {
	return &worker{db: db, config: config}
}

func (w worker) SaveBatchLogMessages(logChannel <-chan kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()
	batch := []LogMessage{}
	log.Printf("Worker started")
	log.Println(w.config.Worker.BatchTime)
	log.Println(time.Duration(w.config.Worker.BatchTime))
	ticker := time.NewTicker(time.Duration(w.config.Worker.BatchTime) * time.Second)

	for {
		select {
		case msg := <-logChannel:
			var logMessage LogMessage
			if err := json.Unmarshal(msg.Value, &logMessage); err != nil {
				log.Printf("Error parse json: %v", err)
				continue
			}

			parsedTime, err := time.Parse(time.RFC3339, logMessage.Timestamp)
			if err != nil {
				log.Printf("Error parse time: %v", err)
				continue
			}
			logMessage.Timestamp = parsedTime.Format("2006-01-02 15:04:05")
			fmt.Println(logMessage, len(batch))
			batch = append(batch, logMessage)

			if len(batch) >= w.config.Worker.BatchSize {
				w.insertBatch(batch)
				batch = nil
			}

		case <-ticker.C:
			fmt.Println("Time ended")
			if len(batch) > 0 {
				w.insertBatch(batch)
				batch = nil
			}
		}
	}
}

func (w worker) insertBatch(logs []LogMessage) {
	if len(logs) == 0 {
		return
	}

	ctx := context.Background()

	batch, err := w.db.PrepareBatch(ctx, "INSERT INTO logs (service, level, message, timestamp) VALUES (?, ?, ?, ?)")
	if err != nil {
		log.Fatalf("Error preparing batch: %v", err)
		return
	}

	for _, logMsg := range logs {
		if err := batch.Append(logMsg.Service, logMsg.Level, logMsg.Message, logMsg.Timestamp); err != nil {
			log.Printf("Error appending data to batch: %v", err)
			return
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("Error sending batch: %v", err)
	} else {
		fmt.Printf("Saved %d log messages in ClickHouse\n", len(logs))
	}
}
