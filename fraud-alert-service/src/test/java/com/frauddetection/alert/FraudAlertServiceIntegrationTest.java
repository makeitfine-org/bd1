package com.frauddetection.alert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.frauddetection.alert.model.FraudAlert;
import com.frauddetection.alert.service.FraudAlertConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration test for Fraud Alert Service using Testcontainers Kafka.
 * Verifies that alerts published to the fraud-alerts Kafka topic are consumed
 * and served via the dashboard REST API.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Testcontainers
class FraudAlertServiceIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.7.0");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private FraudAlertConsumer fraudAlertConsumer;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void shouldConsumeAlertFromKafkaAndServeViaDashboard() throws Exception {
        // Given: a fraud alert message
        FraudAlert alert = FraudAlert.builder()
                .alertId(UUID.randomUUID().toString())
                .transactionId(UUID.randomUUID().toString())
                .userId("fraudster-user-1")
                .reason("Single transaction exceeds $5000 threshold")
                .score(0.85)
                .transactionAmount(new BigDecimal("9500.00"))
                .windowTotal(new BigDecimal("15000.00"))
                .merchantId("suspicious-merch")
                .location("Unknown City")
                .detectedAt(Instant.now())
                .severity("HIGH")
                .build();

        String alertJson = objectMapper.writeValueAsString(alert);

        // When: publish to fraud-alerts topic
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>("fraud-alerts", alert.getUserId(), alertJson)).get();
        }

        // Then: wait for the consumer to pick it up
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            org.assertj.core.api.Assertions.assertThat(fraudAlertConsumer.getAlertCount()).isGreaterThan(0);

            var alerts = fraudAlertConsumer.getAlertsByUser("fraudster-user-1");
            org.assertj.core.api.Assertions.assertThat(alerts).isNotEmpty();
            org.assertj.core.api.Assertions.assertThat(alerts.get(0).getReason()).contains("$5000");
        });

        // And: verify the dashboard API returns the alert
        mockMvc.perform(get("/api/alerts").param("userId", "fraudster-user-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(1))))
                .andExpect(jsonPath("$[0].userId").value("fraudster-user-1"))
                .andExpect(jsonPath("$[0].severity").value("HIGH"));
    }

    @Test
    void shouldReturnEmptyListWhenNoAlerts() throws Exception {
        mockMvc.perform(get("/api/alerts").param("userId", "non-existent-user"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));
    }

    @Test
    void shouldReturnStatsEndpoint() throws Exception {
        mockMvc.perform(get("/api/alerts/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalAlerts").isNumber());
    }

    @Test
    void healthCheckShouldReturnOk() throws Exception {
        mockMvc.perform(get("/api/alerts/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("Fraud Alert Service is running"));
    }
}
