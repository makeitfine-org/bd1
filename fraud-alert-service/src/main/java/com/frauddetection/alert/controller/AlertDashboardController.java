package com.frauddetection.alert.controller;

import com.frauddetection.alert.model.FraudAlert;
import com.frauddetection.alert.service.FraudAlertConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller providing a fraud alert dashboard API.
 */
@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertDashboardController {

    private final FraudAlertConsumer fraudAlertConsumer;

    /**
     * Retrieves all fraud alerts. Optionally filter by userId or severity.
     */
    @GetMapping
    public ResponseEntity<List<FraudAlert>> getAlerts(
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) String severity) {

        List<FraudAlert> alerts;
        if (userId != null && !userId.isBlank()) {
            alerts = fraudAlertConsumer.getAlertsByUser(userId);
        } else if (severity != null && !severity.isBlank()) {
            alerts = fraudAlertConsumer.getAlertsBySeverity(severity);
        } else {
            alerts = fraudAlertConsumer.getAllAlerts();
        }
        return ResponseEntity.ok(alerts);
    }

    /**
     * Returns alert statistics.
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        List<FraudAlert> all = fraudAlertConsumer.getAllAlerts();
        long critical = all.stream().filter(a -> "CRITICAL".equalsIgnoreCase(a.getSeverity())).count();
        long high = all.stream().filter(a -> "HIGH".equalsIgnoreCase(a.getSeverity())).count();
        long medium = all.stream().filter(a -> "MEDIUM".equalsIgnoreCase(a.getSeverity())).count();

        return ResponseEntity.ok(Map.of(
                "totalAlerts", all.size(),
                "critical", critical,
                "high", high,
                "medium", medium
        ));
    }

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Fraud Alert Service is running");
    }
}
