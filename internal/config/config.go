package config

import (
	"os"
	"strconv"
)

type Config struct {
	Kafka      *Kafka
	ClickHouse *ClickHouse
	Worker     *Worker
}

type Kafka struct {
	Topic   string
	Broker  string
	GroupID string
}

type ClickHouse struct {
	Addr     string
	Username string
	Password string
	Database string
}

type Worker struct {
	Count     int
	BatchSize int
	BatchTime int
}

func NewConfig() *Config {
	return &Config{
		Kafka: &Kafka{
			Topic:   getEnv("KAFKA_TOPIC", "logs_topic"),
			Broker:  getEnv("KAFKA_BROKER", "localhost:9092"),
			GroupID: getEnv("KAFKA_GROUPID", "log-consumer-group"),
		},
		ClickHouse: &ClickHouse{
			Addr:     getEnv("CLICKHOUSE_SERVER_ADDR", "localhost:9090"),
			Username: getEnv("CLICKHOUSE_USERNAME", "default"),
			Password: getEnv("CLICKHOUSE_PASSWORD", ""),
			Database: getEnv("CLICKHOUSE_DATABASE", "logs"),
		},
		Worker: &Worker{
			Count:     getEnvAsInt("WORKER_COUNT", 3),
			BatchSize: getEnvAsInt("WORKER_BATCH_SIZE", 500),
			BatchTime: getEnvAsInt("WORKER_BATCH_TIME", 7200),
		},
	}
}

func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return defaultVal
}

func getEnvAsInt(name string, defaultVal int) int {
	valueStr := getEnv(name, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}

	return defaultVal
}

func getEnvAsBool(name string, defaultVal bool) bool {
	valStr := getEnv(name, "")
	if val, err := strconv.ParseBool(valStr); err == nil {
		return val
	}

	return defaultVal
}
