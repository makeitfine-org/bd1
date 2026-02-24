# Real-time Financial Fraud Detection System

A Maven multi-module Java 21 project implementing a full data pipeline for financial fraud detection.
The system ingests transactions via REST, streams them through Kafka, applies real-time fraud scoring with Flink,
stores data in Hadoop HDFS, runs ML batch analytics with Spark, and exposes fraud alerts via a dashboard service.

## Proposed Changes

### Project Root

#### [NEW] [pom.xml](file:///home/eug/.gemini/antigravity/playground/inner-spicule/pom.xml)
Parent POM with Java 21, Spring Boot 3.4, Flink 1.19, Spark 3.5, Hadoop 3.3 dependency management. Modules:
- `transaction-api`
- `fraud-alert-service`
- `flink-fraud-detector`
- `spark-analytics`

#### [NEW] [docker-compose.yml](file:///home/eug/.gemini/antigravity/playground/inner-spicule/docker-compose.yml)
Full Docker Compose with:
- Zookeeper + Kafka (bitnami)
- Kafka topics init container
- Hadoop NameNode + DataNode (apache/hadoop)
- Flink JobManager + TaskManager
- Spark Master + Worker
- `transaction-api` (port 8080)
- `fraud-alert-service` (port 8082)

---

### Module: transaction-api

Spring Boot 3.4 microservice that provides a REST API for submitting transactions and publishes them to Kafka.

#### [NEW] pom.xml
Deps: `spring-boot-starter-web`, `spring-kafka`, `lombok`, `jackson-databind`

#### [NEW] `TransactionApiApplication.java`
#### [NEW] `Transaction.java` — Record/DTO with: `id`, `userId`, `amount`, `merchantId`, `timestamp`, `location`
#### [NEW] `TransactionController.java` — `POST /api/transactions` endpoint
#### [NEW] `TransactionProducerService.java` — publishes to topic `raw-transactions`
#### [NEW] `application.yml` — Kafka bootstrap, topic config
#### [NEW] `TransactionApiIntegrationTest.java` — Testcontainers Kafka + HTTP POST test

---

### Module: fraud-alert-service

Spring Boot 3.4 microservice that consumes from `fraud-alerts` Kafka topic and serves an in-memory alert dashboard.

#### [NEW] pom.xml
Deps: `spring-boot-starter-web`, `spring-kafka`, `lombok`

#### [NEW] `FraudAlertServiceApplication.java`
#### [NEW] `FraudAlert.java` — DTO: `transactionId`, `userId`, `reason`, `detectedAt`, `score`
#### [NEW] `FraudAlertConsumer.java` — `@KafkaListener` on `fraud-alerts` topic
#### [NEW] `AlertDashboardController.java` — `GET /api/alerts` endpoint
#### [NEW] `application.yml`
#### [NEW] `FraudAlertServiceIntegrationTest.java` — Testcontainers Kafka test

---

### Module: flink-fraud-detector

Flink 1.19 job consuming `raw-transactions`, applying tumbling window fraud rules, outputting to `fraud-alerts` and HDFS.

#### [NEW] pom.xml
Deps: `flink-streaming-java`, `flink-connector-kafka`, `flink-connector-filesystem`, `jackson-databind`

#### [NEW] `FlinkFraudDetectorJob.java`
- Kafka source on `raw-transactions`
- KeyBy `userId`, 1-minute tumbling window
- `FraudScoringFunction` — flags if sum > $10,000 in window, or single tx > $5,000
- Kafka sink to `fraud-alerts`
- FileSink to HDFS path `/fraud-detection/raw-transactions/`

#### [NEW] `FraudScoringFunction.java` — ProcessWindowFunction with scoring logic
#### [NEW] `FlinkFraudDetectorIntegrationTest.java` — Testcontainers Kafka + embedded Flink MiniCluster test

---

### Module: spark-analytics

Spark 3.5 application with two jobs: Kafka→HDFS streaming ingest and batch ML fraud model training.

#### [NEW] pom.xml
Deps: `spark-core`, `spark-sql`, `spark-streaming-kafka-0-10`, `spark-mllib`

#### [NEW] `KafkaToHdfsStreamingJob.java` — Structured Streaming, reads `raw-transactions`, writes Parquet to HDFS
#### [NEW] `FraudModelTrainingJob.java` — Reads historical Parquet from HDFS, trains Logistic Regression using MLlib, saves model
#### [NEW] `SparkAnalyticsRunner.java` — Main entry point, selects job by arg
#### [NEW] `SparkAnalyticsIntegrationTest.java` — Testcontainers Kafka + local Spark (local[*]) test

---

## Verification Plan

### Automated Tests

All integration tests use **Testcontainers** and run with:

```bash
# From project root — run all tests
mvn verify -pl transaction-api,fraud-alert-service,flink-fraud-detector,spark-analytics

# Or per module
mvn -pl transaction-api verify
mvn -pl fraud-alert-service verify
mvn -pl flink-fraud-detector verify
mvn -pl spark-analytics verify
```

| Test Class | Coverage |
|---|---|
| `TransactionApiIntegrationTest` | POST /api/transactions → Kafka message produced |
| `FraudAlertServiceIntegrationTest` | Kafka message published → consumed, GET /api/alerts returns alert |
| `FlinkFraudDetectorIntegrationTest` | Kafka input → Flink MiniCluster window → Kafka fraud-alert output |
| `SparkAnalyticsIntegrationTest` | Kafka source → Spark local write to temp HDFS path |

### Manual Verification

```bash
# 1. Start all services
docker compose up -d

# 2. Submit a suspicious transaction (amount > $5000 triggers fraud)
curl -X POST http://localhost:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{"userId":"user-1","amount":9500.00,"merchantId":"merch-42","location":"New York"}'

# 3. Check fraud alerts dashboard (wait ~5 seconds for Flink to process)
curl http://localhost:8082/api/alerts
```
