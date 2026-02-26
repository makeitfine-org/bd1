# Plan: Fix Flink Fraud Detector Integration Test Failure

## Context
The `FlinkFraudDetectorIntegrationTest` fails with a `ConditionTimeoutException` — it waits 60 seconds for fraud alerts on the `fraud-alerts` Kafka topic but receives nothing. The test logic is correct (sending a $7,500 transaction and expecting a HIGH-severity alert). **The functionality must be fixed, not the test.**

## Root Cause

Two compounding problems in `FlinkFraudDetectorJob.buildPipeline()`:

### Problem 1: Wrong windowing strategy for fraud detection
`TumblingProcessingTimeWindows.of(5s)` only fires **after the window closes**. In the test the Flink job needs to:
1. Start up and connect to Kafka
2. Read all 3 messages (topic may appear as `UNKNOWN_TOPIC_OR_PARTITION` briefly)
3. Assign them to the current processing-time window
4. **Wait for the 5-second boundary to pass**
5. Fire `FraudScoringFunction.process()`

This is fragile in embedded mini-cluster tests because the window may never fire if the source becomes idle (no new messages) and Flink's processing-time service can't advance while the source thread is blocked in a Kafka poll. Additionally Rule 1 (single tx > $5,000) is a stateless per-record check that should NOT require windowing — waiting up to 1 minute in production is unacceptable for fraud detection.

### Problem 2: KafkaSink may not flush without checkpointing
The test creates a fresh `StreamExecutionEnvironment` without calling `env.enableCheckpointing()`. If `KafkaSink` defaults to `AT_LEAST_ONCE`, records are only flushed on checkpoint. Without checkpoints, the fraud alert sits in the producer buffer and never reaches Kafka.

## Fix: Replace window + FraudScoringFunction with a KeyedProcessFunction

Replace the window-based pipeline segment with a `KeyedProcessFunction` that:
1. **Immediately emits** a HIGH alert when any single transaction > $5,000 arrives (Rule 1 — no state needed)
2. Maintains a `ListState<TransactionEvent>` with a 1-minute TTL to evaluate Rules 2 and 3 on each new transaction (window-aggregate behaviour preserved, but evaluated eagerly)

Also explicitly set `KafkaSink` delivery guarantee to `DeliveryGuarantee.NONE` so records are sent without requiring checkpoints.

## Files to Modify

| File | Change |
|------|--------|
| `flink-fraud-detector/src/main/java/com/frauddetection/flink/FlinkFraudDetectorJob.java` | Replace `.window(...).process(new FraudScoringFunction())` with `.process(new FraudScoringFunction())` (KeyedProcessFunction); add `setDeliveryGuarantee(DeliveryGuarantee.NONE)` to KafkaSink |
| `flink-fraud-detector/src/main/java/com/frauddetection/flink/function/FraudScoringFunction.java` | Change base class from `ProcessWindowFunction<TransactionEvent, FraudAlertEvent, String, TimeWindow>` to `KeyedProcessFunction<String, TransactionEvent, FraudAlertEvent>`; use `ListState` + TTL for aggregate rules |

## Implementation Details

### FraudScoringFunction (new signature)
```java
public class FraudScoringFunction
        extends KeyedProcessFunction<String, TransactionEvent, FraudAlertEvent> {

    private transient ListState<TransactionEvent> windowState;

    @Override
    public void open(OpenContext ctx) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.minutes(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        ListStateDescriptor<TransactionEvent> desc =
                new ListStateDescriptor<>("window-txs", TransactionEvent.class);
        desc.enableTimeToLive(ttl);
        windowState = getRuntimeContext().getListState(desc);
    }

    @Override
    public void processElement(TransactionEvent tx, Context ctx, Collector<FraudAlertEvent> out) throws Exception {
        // Rule 1: immediate, stateless
        if (tx.getAmount().compareTo(SINGLE_TX_THRESHOLD) > 0) {
            out.collect(buildAlert(tx, tx.getAmount(), "HIGH", 0.85, ...));
        }
        // Update state for aggregate rules
        windowState.add(tx);
        List<TransactionEvent> all = StreamSupport.stream(windowState.get().spliterator(), false)
                .collect(Collectors.toList());
        BigDecimal total = all.stream().map(TransactionEvent::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        // Rule 2: window total
        if (total.compareTo(WINDOW_TOTAL_THRESHOLD) > 0) { ... }
        // Rule 3: velocity
        if (all.size() > VELOCITY_THRESHOLD) { ... }
    }
}
```

### FlinkFraudDetectorJob pipeline change
```java
// Before:
.window(TumblingProcessingTimeWindows.of(windowSize))
.process(new FraudScoringFunction())

// After:
.process(new FraudScoringFunction())   // KeyedProcessFunction, no window
```

Remove the `windowSize` parameter from `buildPipeline` (or keep it but ignore it / use it as state TTL).

```java
// KafkaSink — add delivery guarantee
KafkaSink.<FraudAlertEvent>builder()
    .setBootstrapServers(kafkaBootstrap)
    .setDeliveryGuarantee(DeliveryGuarantee.NONE)
    .setRecordSerializer(...)
    .build();
```

## Verification
Run `mvn clean install -pl flink-fraud-detector` — `FlinkFraudDetectorIntegrationTest.shouldDetectFraudulentTransactions` should pass (alert for user-A, $7500.00 received within 60 seconds, now effectively within a few seconds of the Flink job starting).
