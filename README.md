# Real-time Financial Fraud Detection System

A comprehensive real-time financial fraud detection platform built with **Spring Boot**, **Kafka**, **Apache Flink**, **Apache Spark**, and **Hadoop HDFS**.

## Architecture

```
┌─────────────────┐    ┌─────────┐    ┌──────────────────┐    ┌─────────┐    ┌──────────────────┐
│ Transaction API │───▶│  Kafka  │───▶│ Flink Fraud      │───▶│  Kafka  │───▶│ Fraud Alert      │
│ (Spring Boot)   │    │ raw-tx  │    │ Detector         │    │ alerts  │    │ Service          │
│ POST /api/tx    │    │         │    │ Tumbling Window  │    │         │    │ GET /api/alerts  │
└─────────────────┘    └────┬────┘    └──────────────────┘    └─────────┘    └──────────────────┘
                            │
                            ▼
                    ┌──────────────────┐    ┌─────────┐
                    │ Spark Analytics  │───▶│  HDFS   │
                    │ Streaming + ML   │    │ Parquet │
                    └──────────────────┘    └─────────┘
```

## Modules

| Module | Description | Port |
|--------|-------------|------|
| `transaction-api` | REST API for transaction submission + Kafka producer | 8080 |
| `fraud-alert-service` | Kafka consumer + fraud alert dashboard API | 8082 |
| `flink-fraud-detector` | Real-time fraud scoring with 1-min tumbling window | - |
| `spark-analytics` | Kafka→HDFS streaming + ML model training (LogReg) | - |

## Tech Stack

- **Java 21** + **Maven**
- **Spring Boot 3.4.3**
- **Apache Kafka 3.7** (via Bitnami)
- **Apache Flink 1.19.1**
- **Apache Spark 3.5.4** with MLlib
- **Hadoop 3.3.6** (HDFS)
- **Testcontainers** for integration tests

## Fraud Detection Rules (Flink)

| Rule | Threshold | Severity | Score |
|------|-----------|----------|-------|
| Single high-value transaction | > $5,000 | HIGH | 0.85 |
| Window total exceeds limit | > $10,000/min | CRITICAL | 0.95 |
| High-velocity transactions | > 5 tx/min | MEDIUM | 0.60 |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 21 + Maven 3.9+

### Build
```bash
mvn clean package -DskipTests
```

### Run with Docker Compose
```bash
docker compose up -d
```

### Submit a Transaction
```bash
curl -X POST http://localhost:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{
    "userId": "user-1",
    "amount": 9500.00,
    "merchantId": "merch-42",
    "location": "New York"
  }'
```

### Check Fraud Alerts
```bash
# Wait ~5 seconds for Flink processing, then:
curl http://localhost:8082/api/alerts
curl http://localhost:8082/api/alerts/stats
```

## Running Tests
```bash
# All modules (requires Docker for Testcontainers)
mvn verify

# Per module
mvn -pl transaction-api verify
mvn -pl fraud-alert-service verify
mvn -pl flink-fraud-detector verify
mvn -pl spark-analytics verify
```

## Service URLs (Docker Compose)

| Service | URL |
|---------|-----|
| Transaction API | http://localhost:8080/api/transactions |
| Fraud Alert Dashboard | http://localhost:8082/api/alerts |
| Flink Dashboard | http://localhost:8081 |
| Spark Master UI | http://localhost:8083 |
| HDFS NameNode UI | http://localhost:9870 |
