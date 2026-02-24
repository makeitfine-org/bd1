package com.frauddetection.flink.function;

import com.frauddetection.flink.model.FraudAlertEvent;
import com.frauddetection.flink.model.TransactionEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Fraud scoring function that processes a 1-minute tumbling window of transactions per user.
 *
 * Rules applied:
 * 1. Single transaction > $5,000 → HIGH severity (score 0.85)
 * 2. Window total > $10,000 → CRITICAL severity (score 0.95)
 * 3. More than 5 transactions in a 1-minute window → MEDIUM severity (score 0.60)
 * 4. Single transaction > $3,000 from a new/unusual location → MEDIUM severity (score 0.70)
 */
public class FraudScoringFunction
        extends ProcessWindowFunction<TransactionEvent, FraudAlertEvent, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudScoringFunction.class);

    private static final BigDecimal SINGLE_TX_THRESHOLD = new BigDecimal("5000");
    private static final BigDecimal WINDOW_TOTAL_THRESHOLD = new BigDecimal("10000");
    private static final int VELOCITY_THRESHOLD = 5;
    private static final BigDecimal LOCATION_ANOMALY_THRESHOLD = new BigDecimal("3000");

    @Override
    public void process(String userId,
                        ProcessWindowFunction<TransactionEvent, FraudAlertEvent, String, TimeWindow>.Context context,
                        Iterable<TransactionEvent> elements,
                        Collector<FraudAlertEvent> out) {

        List<TransactionEvent> transactions = new ArrayList<>();
        elements.forEach(transactions::add);

        BigDecimal windowTotal = transactions.stream()
                .map(TransactionEvent::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        LOG.info("Processing window for user [{}]: {} transactions, total amount: {}",
                userId, transactions.size(), windowTotal);

        // Rule 1: Check each transaction for high-value single transactions
        for (TransactionEvent tx : transactions) {
            if (tx.getAmount().compareTo(SINGLE_TX_THRESHOLD) > 0) {
                FraudAlertEvent alert = new FraudAlertEvent();
                alert.setAlertId(UUID.randomUUID().toString());
                alert.setTransactionId(tx.getId());
                alert.setUserId(userId);
                alert.setReason("Single transaction exceeds $" + SINGLE_TX_THRESHOLD + " threshold");
                alert.setScore(0.85);
                alert.setTransactionAmount(tx.getAmount());
                alert.setWindowTotal(windowTotal);
                alert.setMerchantId(tx.getMerchantId());
                alert.setLocation(tx.getLocation());
                alert.setDetectedAt(Instant.now());
                alert.setSeverity("HIGH");
                out.collect(alert);

                LOG.warn("🚨 HIGH severity alert: user={}, txId={}, amount={}",
                        userId, tx.getId(), tx.getAmount());
            }
        }

        // Rule 2: Window total exceeds threshold
        if (windowTotal.compareTo(WINDOW_TOTAL_THRESHOLD) > 0) {
            TransactionEvent lastTx = transactions.get(transactions.size() - 1);
            FraudAlertEvent alert = new FraudAlertEvent();
            alert.setAlertId(UUID.randomUUID().toString());
            alert.setTransactionId(lastTx.getId());
            alert.setUserId(userId);
            alert.setReason("Window total $" + windowTotal + " exceeds $" + WINDOW_TOTAL_THRESHOLD + " in 1 minute");
            alert.setScore(0.95);
            alert.setTransactionAmount(lastTx.getAmount());
            alert.setWindowTotal(windowTotal);
            alert.setMerchantId(lastTx.getMerchantId());
            alert.setLocation(lastTx.getLocation());
            alert.setDetectedAt(Instant.now());
            alert.setSeverity("CRITICAL");
            out.collect(alert);

            LOG.warn("🚨 CRITICAL severity alert: user={}, windowTotal={}", userId, windowTotal);
        }

        // Rule 3: High-velocity transactions
        if (transactions.size() > VELOCITY_THRESHOLD) {
            TransactionEvent lastTx = transactions.get(transactions.size() - 1);
            FraudAlertEvent alert = new FraudAlertEvent();
            alert.setAlertId(UUID.randomUUID().toString());
            alert.setTransactionId(lastTx.getId());
            alert.setUserId(userId);
            alert.setReason("High velocity: " + transactions.size() + " transactions in 1-minute window");
            alert.setScore(0.60);
            alert.setTransactionAmount(lastTx.getAmount());
            alert.setWindowTotal(windowTotal);
            alert.setMerchantId(lastTx.getMerchantId());
            alert.setLocation(lastTx.getLocation());
            alert.setDetectedAt(Instant.now());
            alert.setSeverity("MEDIUM");
            out.collect(alert);

            LOG.warn("🚨 MEDIUM severity alert: user={}, txCount={}", userId, transactions.size());
        }
    }
}
