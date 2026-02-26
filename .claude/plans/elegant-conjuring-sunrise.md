# Project Analysis & Improvement Plan
## Real-time Financial Fraud Detection System

## Context
The project is a multi-module Maven application (Java 21) for real-time financial fraud detection. It consists of:
- **transaction-api** — Spring Boot REST API, Kafka producer → `raw-transactions` topic
- **fraud-alert-service** — Spring Boot Kafka consumer from `fraud-alerts`, REST dashboard
- **flink-fraud-detector** — Flink 1.19.1 stream processor (3 fraud rules), reads → writes Kafka
- **spark-analytics** — Spark 3.5.4, Kafka-to-HDFS archival + Logistic Regression ML training

The build currently succeeds (`mvn clean install`, all 4 modules, BUILD SUCCESS). All modules have integration tests using Testcontainers. The goal of this plan is to fix critical bugs, improve performance, add observability, and harden the codebase.

---

## Phase 1 — Critical Fixes (Security & Data Integrity)

### 1.1 Fix Deserialization Security Vulnerability
**File:** `fraud-alert-service/src/main/resources/application.yml`

Change `spring.json.trusted.packages: "*"` to the specific package. The wildcard allows arbitrary class deserialization — a potential RCE vector.
```yaml
# Before:
spring.json.trusted.packages: "*"

# After:
spring.json.trusted.packages: "com.frauddetection.alert.model"
```
> Since the consumer uses `StringDeserializer` and deserializes manually via ObjectMapper, this config is actually unused. Remove the entire `spring.json` block and add a comment clarifying manual deserialization is used.

### 1.2 Fix Financial Precision in Spark
**File:** `spark-analytics/src/main/java/com/frauddetection/spark/KafkaToHdfsStreamingJob.java`

`amount` is declared as `DoubleType` — floating-point precision loss is unacceptable for monetary values.
```java
// Before:
.add("amount", DataTypes.DoubleType)

// After:
.add("amount", DataTypes.createDecimalType(18, 2))
```

### 1.3 Add Input Validation to Transaction API
**File:** `transaction-api/src/main/java/com/frauddetection/transaction/model/Transaction.java`

Add Jakarta Bean Validation annotations. `spring-boot-starter-web` already includes the validator.
```java
@NotBlank String userId;
@NotBlank String merchantId;
@NotNull @DecimalMin("0.00") BigDecimal amount;
@NotNull String currency;
```
**File:** `transaction-api/src/main/java/com/frauddetection/transaction/controller/TransactionController.java`
Add `@Valid` to the `@RequestBody` parameter and add `@ExceptionHandler(MethodArgumentNotValidException.class)` returning HTTP 400 with error details.

### 1.4 Improve Flink Delivery Guarantee
**File:** `flink-fraud-detector/src/main/java/com/frauddetection/flink/FlinkFraudDetectorJob.java`

Currently `DeliveryGuarantee.NONE` — fraud alerts can be silently dropped on Flink restarts.
```java
// Before:
.setDeliveryGuarantee(DeliveryGuarantee.NONE)

// After:
.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
```
Also enable checkpointing to support `AT_LEAST_ONCE`:
```java
env.enableCheckpointing(30_000); // 30s (down from 60s)
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
```

---

## Phase 2 — Performance Optimizations

### 2.1 Optimize Flink State Access (Hot Path)
**File:** `flink-fraud-detector/src/main/java/com/frauddetection/flink/function/FraudScoringFunction.java`

Currently the window state is materialized into a full `List<TransactionEvent>` on **every incoming record** — O(n) per event. Replace with two `ValueState` fields for the running sum and count:

```java
// Add alongside windowState:
private transient ValueState<BigDecimal> windowSumState;
private transient ValueState<Integer> windowCountState;

// In open():
ValueStateDescriptor<BigDecimal> sumDesc = new ValueStateDescriptor<>("windowSum", BigDecimal.class);
sumDesc.enableTimeToLive(ttlConfig);
windowSumState = getRuntimeContext().getState(sumDesc);

ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("windowCount", Integer.class);
countDesc.enableTimeToLive(ttlConfig);
windowCountState = getRuntimeContext().getState(countDesc);

// In processElement(): update sum and count incrementally instead of iterating the full list
BigDecimal sum = windowSumState.value() == null ? BigDecimal.ZERO : windowSumState.value();
sum = sum.add(tx.getAmount());
windowSumState.update(sum);
int count = windowCountState.value() == null ? 0 : windowCountState.value();
windowCountState.update(count + 1);
```
Keep `windowState` (ListState) only for Rule 3 (velocity count is now `windowCountState`). Rules 1 and 2 no longer need to iterate the list.

### 2.2 Cache Stats Computation in Alert Service
**File:** `fraud-alert-service/src/main/java/com/frauddetection/alert/service/FraudAlertConsumer.java`

Stats are recomputed by streaming the entire alert list on every `/api/alerts/stats` call. Maintain atomic counters:
```java
private final AtomicInteger totalCount = new AtomicInteger(0);
private final AtomicInteger criticalCount = new AtomicInteger(0);
private final AtomicInteger highCount = new AtomicInteger(0);
private final AtomicInteger mediumCount = new AtomicInteger(0);
```
Increment the appropriate counter when `consumeFraudAlert()` processes each message. Expose a `getStats()` method returning a record/DTO. This turns O(n) into O(1).

### 2.3 Add Memory-Bounded Alert Cache with Eviction
**File:** `fraud-alert-service/src/main/java/com/frauddetection/alert/service/FraudAlertConsumer.java`

Replace unbounded `CopyOnWriteArrayList` with a bounded deque (e.g., keep last 10,000 alerts):
```java
private final int MAX_ALERTS = 10_000;
private final Deque<FraudAlert> alertCache = new ArrayDeque<>();
private final ReadWriteLock lock = new ReentrantReadWriteLock();

// In consumeFraudAlert():
lock.writeLock().lock();
try {
    if (alertCache.size() >= MAX_ALERTS) alertCache.pollFirst();
    alertCache.addLast(alert);
} finally { lock.writeLock().unlock(); }
```
Make limit configurable via `@Value("${app.alerts.max-cache-size:10000}")`.

### 2.4 Add Pagination to GET /api/alerts
**File:** `fraud-alert-service/src/main/java/com/frauddetection/alert/controller/AlertDashboardController.java`

Add `page` and `size` query parameters (default page=0, size=50, max size=200):
```java
@GetMapping("/api/alerts")
public ResponseEntity<Map<String, Object>> getAlerts(
    @RequestParam(required=false) String userId,
    @RequestParam(required=false) String severity,
    @RequestParam(defaultValue="0") int page,
    @RequestParam(defaultValue="50") int size) { ... }
```
Return a response envelope with `{"alerts": [...], "page": 0, "size": 50, "total": 123}`.

---

## Phase 3 — Reliability & Correctness

### 3.1 Replace Severity String with Enum
**Files:** Both `flink-fraud-detector/model/FraudAlertEvent.java` and `fraud-alert-service/model/FraudAlert.java`

Add a shared `Severity` enum: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`. Update all string comparisons and serialization (Jackson handles enums to/from JSON natively).

### 3.2 Improve ML Feature Engineering in Spark
**File:** `spark-analytics/src/main/java/com/frauddetection/spark/FraudModelTrainingJob.java`

Current label: `IF amount > 5000 THEN 1.0` — this just replicates the rule, ML adds no value.

**Improvements:**
1. Add `StandardScaler` after `VectorAssembler` to normalize feature ranges
2. Add temporal features: extract `hour_of_day`, `day_of_week` from timestamp column
3. Add `CrossValidator` with `ParamGridBuilder` for basic hyperparameter tuning (logistic regression `regParam` values)
4. Report precision, recall, and F1 alongside AUC (use `MulticlassClassificationEvaluator`)
5. Save model with metadata file (training date, AUC, feature list) alongside model artifact

### 3.3 Add Watermark Strategy to Flink
**File:** `flink-fraud-detector/src/main/java/com/frauddetection/flink/FlinkFraudDetectorJob.java`

Replace `WatermarkStrategy.noWatermarks()` with event-time watermarking to handle late arrivals:
```java
WatermarkStrategy.<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((event, ts) -> event.getTimestamp().toEpochMilli())
```

### 3.4 Externalize Topic Names and Add Spring Profiles
**Files:** Both `application.yml` files and Flink/Spark job constants

Move all topic names to config. Add `application-dev.yml` and `application-prod.yml` profiles for environment-specific overrides (Kafka broker addresses, replica counts, etc.).

---

## Phase 4 — Observability

### 4.1 Add Spring Boot Actuator
**Files:** Both `transaction-api/pom.xml` and `fraud-alert-service/pom.xml`

Add dependency:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```
**Files:** Both `application.yml` files — expose health and metrics endpoints:
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: always
```

### 4.2 Add Micrometer/Prometheus Metrics
Add `micrometer-registry-prometheus` dependency. In `FraudAlertConsumer`, inject `MeterRegistry` and track:
- `fraud.alerts.total` (Counter, tagged by severity)
- `fraud.alerts.processing.time` (Timer)
In `TransactionProducerService`, track:
- `transactions.submitted.total` (Counter)
- `kafka.publish.latency` (Timer from send to callback)

### 4.3 Structured Logging Configuration
Add `logback-spring.xml` to both Spring Boot modules with:
- JSON log format for production profile (using `logstash-logback-encoder` or plain JSON)
- Console pattern for dev profile
- MDC fields: `transactionId`, `userId`, `severity`

---

## Phase 5 — Testing

### 5.1 Unit Tests for Fraud Scoring Logic
**New file:** `flink-fraud-detector/src/test/java/.../function/FraudScoringFunctionTest.java`

Use Flink's `KeyedOneInputStreamOperatorTestHarness` to unit-test `FraudScoringFunction` in isolation:
- Test Rule 1: single transaction > $5,000 → HIGH alert
- Test Rule 2: window total > $10,000 → CRITICAL alert
- Test Rule 3: > 5 transactions in window → MEDIUM alert
- Test no alert below thresholds
- Test TTL expiry clears state

### 5.2 Unit Tests for TransactionProducerService
**New file:** `transaction-api/src/test/.../service/TransactionProducerServiceTest.java`

Mock `KafkaTemplate`, verify:
- Correct topic routing
- UserId used as message key
- JsonProcessingException propagates correctly

### 5.3 Negative Test Cases for Transaction API
**File:** `transaction-api/src/test/.../TransactionApiIntegrationTest.java`

Add tests for:
- POST with negative amount → 400 Bad Request
- POST with missing `userId` → 400 Bad Request
- POST with missing `amount` → 400 Bad Request
- Verify error response body contains field-level error messages

### 5.4 Fix Slow Test Timeouts
**Files:** All integration test files

Reduce Awaitility timeouts from 30s/60s to 10s with faster poll intervals (250ms). Use `@Timeout` JUnit 5 annotation on test class for hard ceiling.

---

## Critical Files to Modify

| File | Changes |
|------|---------|
| `fraud-alert-service/src/main/resources/application.yml` | Fix trusted packages |
| `spark-analytics/.../KafkaToHdfsStreamingJob.java` | Fix DecimalType for amount |
| `transaction-api/.../model/Transaction.java` | Add Bean Validation annotations |
| `transaction-api/.../controller/TransactionController.java` | Add @Valid, error handler |
| `flink-fraud-detector/.../FlinkFraudDetectorJob.java` | AT_LEAST_ONCE, watermarks |
| `flink-fraud-detector/.../FraudScoringFunction.java` | ValueState for sum/count |
| `fraud-alert-service/.../service/FraudAlertConsumer.java` | Bounded cache, atomic counters |
| `fraud-alert-service/.../controller/AlertDashboardController.java` | Pagination |
| `spark-analytics/.../FraudModelTrainingJob.java` | StandardScaler, better metrics |
| `transaction-api/pom.xml` + `fraud-alert-service/pom.xml` | Add Actuator |
| Both `application.yml` | Actuator config, profiles |

---

## Verification

1. **Build:** `mvn clean install` — all modules must pass
2. **Integration tests:** All 4 existing integration tests must pass
3. **New unit tests:** `mvn test -pl flink-fraud-detector` and `mvn test -pl transaction-api`
4. **Manual validation:** `docker compose up`, submit test transaction via `curl -X POST localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"userId":"u1","merchantId":"m1","amount":7500}'` → alert appears at `localhost:8082/api/alerts`
5. **Validation check:** POST with `amount: -100` must return HTTP 400
6. **Actuator check:** `curl localhost:8080/actuator/health` must return `{"status":"UP"}`
7. **Stats check:** `curl localhost:8082/api/alerts/stats` must return O(1) response
