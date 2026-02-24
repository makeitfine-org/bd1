# Real-time Financial Fraud Detection - Task Checklist

## Planning
- [x] Define system architecture and project structure
- [x] Write implementation plan

## Project Structure
- [ ] Create Maven multi-module parent POM
- [ ] Set up module directories

## Module: transaction-api (Spring Boot - Producer)
- [ ] pom.xml
- [ ] Main application class
- [ ] Transaction model/DTO
- [ ] REST Controller (POST /api/transactions)
- [ ] Kafka producer service
- [ ] application.yml

## Module: fraud-alert-service (Spring Boot - Consumer)
- [ ] pom.xml
- [ ] Main application class
- [ ] Kafka consumer for fraud-alerts topic
- [ ] Dashboard/REST endpoint for viewing alerts
- [ ] Alert model
- [ ] application.yml

## Module: flink-fraud-detector (Flink)
- [ ] pom.xml
- [ ] Main Flink job class
- [ ] Kafka source connector setup
- [ ] Tumbling window fraud scoring logic (1-min window)
- [ ] Fraud alert sink (Kafka)
- [ ] HDFS sink for raw data

## Module: spark-analytics (Spark)
- [ ] pom.xml
- [ ] SparkSession setup
- [ ] Kafka to HDFS streaming job
- [ ] Batch ML training job (Logistic Regression)
- [ ] Analytics job runner

## Docker Compose
- [ ] Zookeeper
- [ ] Kafka broker
- [ ] Hadoop (NameNode + DataNode)
- [ ] Flink (JobManager + TaskManager)
- [ ] Spark (Master + Worker)
- [ ] transaction-api service
- [ ] fraud-alert-service

## Integration Tests (Testcontainers)
- [ ] transaction-api integration test (Kafka TC)
- [ ] fraud-alert-service integration test (Kafka TC)
- [ ] Flink job integration test
- [ ] Spark analytics integration test

## Verification
- [ ] Build all modules
- [ ] Verify Docker Compose configuration
- [ ] Run integration tests
