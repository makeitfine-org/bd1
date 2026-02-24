package com.frauddetection.alert.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.alert.model.FraudAlert;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Consumes fraud alert events from the fraud-alerts Kafka topic.
 * Stores alerts in-memory for dashboard consumption.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FraudAlertConsumer {

    private final ObjectMapper objectMapper;
    private final CopyOnWriteArrayList<FraudAlert> alerts = new CopyOnWriteArrayList<>();

    @KafkaListener(
            topics = "${app.kafka.topic.fraud-alerts}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeFraudAlert(String message) {
        try {
            FraudAlert alert = objectMapper.readValue(message, FraudAlert.class);
            alerts.add(alert);
            log.warn("🚨 FRAUD ALERT: userId={}, reason={}, score={}, amount={}, severity={}",
                    alert.getUserId(), alert.getReason(), alert.getScore(),
                    alert.getTransactionAmount(), alert.getSeverity());
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize fraud alert: {}", message, e);
        }
    }

    /**
     * Returns all received fraud alerts.
     */
    public List<FraudAlert> getAllAlerts() {
        return List.copyOf(alerts);
    }

    /**
     * Returns alerts filtered by userId.
     */
    public List<FraudAlert> getAlertsByUser(String userId) {
        return alerts.stream()
                .filter(a -> userId.equals(a.getUserId()))
                .toList();
    }

    /**
     * Returns alerts filtered by severity.
     */
    public List<FraudAlert> getAlertsBySeverity(String severity) {
        return alerts.stream()
                .filter(a -> severity.equalsIgnoreCase(a.getSeverity()))
                .toList();
    }

    /**
     * Returns the total number of alerts received.
     */
    public int getAlertCount() {
        return alerts.size();
    }
}
