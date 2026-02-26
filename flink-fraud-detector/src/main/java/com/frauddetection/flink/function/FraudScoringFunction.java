package com.frauddetection.flink.function;

import com.frauddetection.flink.model.FraudAlertEvent;
import com.frauddetection.flink.model.TransactionEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Fraud scoring function that evaluates transactions per user using keyed state.
 *
 * Rules applied:
 * 1. Single transaction > $5,000 → HIGH severity (score 0.85) — emitted immediately
 * 2. Rolling 1-minute total > $10,000 → CRITICAL severity (score 0.95)
 * 3. More than 5 transactions in a 1-minute rolling window → MEDIUM severity (score 0.60)
 */
public class FraudScoringFunction
        extends KeyedProcessFunction<String, TransactionEvent, FraudAlertEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudScoringFunction.class);

    private static final BigDecimal SINGLE_TX_THRESHOLD = new BigDecimal("5000");
    private static final BigDecimal WINDOW_TOTAL_THRESHOLD = new BigDecimal("10000");
    private static final int VELOCITY_THRESHOLD = 5;

    private transient ListState<TransactionEvent> windowState;

    @Override
    public void open(Configuration parameters) throws Exception {
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
        // Rule 1: immediate, stateless check
        if (tx.getAmount().compareTo(SINGLE_TX_THRESHOLD) > 0) {
            FraudAlertEvent alert = new FraudAlertEvent();
            alert.setAlertId(UUID.randomUUID().toString());
            alert.setTransactionId(tx.getId());
            alert.setUserId(tx.getUserId());
            alert.setReason("Single transaction exceeds $" + SINGLE_TX_THRESHOLD + " threshold");
            alert.setScore(0.85);
            alert.setTransactionAmount(tx.getAmount());
            alert.setWindowTotal(tx.getAmount());
            alert.setMerchantId(tx.getMerchantId());
            alert.setLocation(tx.getLocation());
            alert.setDetectedAt(Instant.now());
            alert.setSeverity("HIGH");
            out.collect(alert);

            LOG.warn("HIGH severity alert: user={}, txId={}, amount={}",
                    tx.getUserId(), tx.getId(), tx.getAmount());
        }

        // Update rolling state for aggregate rules
        windowState.add(tx);
        List<TransactionEvent> all = StreamSupport.stream(windowState.get().spliterator(), false)
                .collect(Collectors.toList());

        BigDecimal windowTotal = all.stream()
                .map(TransactionEvent::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        LOG.info("Processing transaction for user [{}]: {} transactions in window, total amount: {}",
                tx.getUserId(), all.size(), windowTotal);

        // Rule 2: Rolling window total exceeds threshold
        if (windowTotal.compareTo(WINDOW_TOTAL_THRESHOLD) > 0) {
            FraudAlertEvent alert = new FraudAlertEvent();
            alert.setAlertId(UUID.randomUUID().toString());
            alert.setTransactionId(tx.getId());
            alert.setUserId(tx.getUserId());
            alert.setReason("Window total $" + windowTotal + " exceeds $" + WINDOW_TOTAL_THRESHOLD + " in 1 minute");
            alert.setScore(0.95);
            alert.setTransactionAmount(tx.getAmount());
            alert.setWindowTotal(windowTotal);
            alert.setMerchantId(tx.getMerchantId());
            alert.setLocation(tx.getLocation());
            alert.setDetectedAt(Instant.now());
            alert.setSeverity("CRITICAL");
            out.collect(alert);

            LOG.warn("CRITICAL severity alert: user={}, windowTotal={}", tx.getUserId(), windowTotal);
        }

        // Rule 3: High-velocity transactions
        if (all.size() > VELOCITY_THRESHOLD) {
            FraudAlertEvent alert = new FraudAlertEvent();
            alert.setAlertId(UUID.randomUUID().toString());
            alert.setTransactionId(tx.getId());
            alert.setUserId(tx.getUserId());
            alert.setReason("High velocity: " + all.size() + " transactions in 1-minute window");
            alert.setScore(0.60);
            alert.setTransactionAmount(tx.getAmount());
            alert.setWindowTotal(windowTotal);
            alert.setMerchantId(tx.getMerchantId());
            alert.setLocation(tx.getLocation());
            alert.setDetectedAt(Instant.now());
            alert.setSeverity("MEDIUM");
            out.collect(alert);

            LOG.warn("MEDIUM severity alert: user={}, txCount={}", tx.getUserId(), all.size());
        }
    }
}
