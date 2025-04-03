# Kafka ClickHouse Consumer

## 📌 Description

This **Kafka Consumer** is written in **Go**, receives logs from **Kafka**, processes them, and stores them in **ClickHouse**. It uses **goroutines** and **batch insert** for high performance.

---

## 🚀 Running the Consumer

### 1️⃣ Install dependencies
```sh
go mod tidy
```

### 2️⃣ Configure `.env`
Create a `.env` file and set the variables:


### 3️⃣ Start the Consumer
```sh
go run main.go
```

---

## ⚙ Configuration

| Variable            | Description                               | Default Value          |
|---------------------|----------------------------------------|------------------------|
| `KAFKA_BROKER`     | Kafka broker address                   | `localhost:9092`      |
| `KAFKA_TOPIC`      | Kafka topic name                       | `logs`                |
| `KAFKA_GROUP_ID`   | Kafka Consumer group ID                | `log-consumer-group`  |
| `CLICKHOUSE_DSN`   | ClickHouse connection string           | `tcp://localhost:9000`|
| `WORKER_BATCH_SIZE` | Number of logs per batch before insert | `100`                 |
| `WORKER_BATCH_TIME` | Time in seconds between DB inserts     | `120`                 |

---

## 🛠 Architecture
1. **Kafka Consumer** connects to the broker and reads messages.
2. Logs are added to a **channel queue** for processing.
3. **Worker Pool** batches logs and inserts them into **ClickHouse**.
4. **Batch Insert** improves performance.

```plaintext
Kafka -> Consumer -> Channel -> Worker Pool -> ClickHouse
```

---


## 📜 License
MIT License.

