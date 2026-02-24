package com.frauddetection.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.frauddetection.transaction.model.Transaction;
import com.frauddetection.transaction.model.TransactionResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration test for Transaction API using Testcontainers Kafka.
 * Verifies that submitting a transaction via REST produces a message on Kafka.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Testcontainers
class TransactionApiIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.7.0");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void shouldSubmitTransactionAndProduceToKafka() throws Exception {
        // Given: a transaction to submit
        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID().toString())
                .userId("user-integration-test")
                .amount(new BigDecimal("7500.00"))
                .merchantId("merch-001")
                .location("New York")
                .currency("USD")
                .timestamp(Instant.now())
                .build();

        String jsonPayload = objectMapper.writeValueAsString(transaction);

        // When: POST to /api/transactions
        MvcResult result = mockMvc.perform(post("/api/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonPayload))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.transactionId").value(transaction.getId()))
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andReturn();

        TransactionResponse response = objectMapper.readValue(
                result.getResponse().getContentAsString(), TransactionResponse.class);
        assertThat(response.getTransactionId()).isEqualTo(transaction.getId());

        // Then: verify the message was produced to Kafka
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("raw-transactions"));

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                assertThat(records.count()).isGreaterThan(0);

                boolean found = false;
                for (var record : records) {
                    Transaction consumed = objectMapper.readValue(record.value(), Transaction.class);
                    if (transaction.getId().equals(consumed.getId())) {
                        assertThat(consumed.getUserId()).isEqualTo("user-integration-test");
                        assertThat(consumed.getAmount()).isEqualByComparingTo(new BigDecimal("7500.00"));
                        found = true;
                        break;
                    }
                }
                assertThat(found).isTrue();
            });
        }
    }

    @Test
    void shouldAutoAssignIdAndTimestamp() throws Exception {
        // Given: a transaction without ID and timestamp
        String jsonPayload = """
                {
                    "userId": "user-no-id",
                    "amount": 150.00,
                    "merchantId": "merch-002",
                    "location": "London"
                }
                """;

        // When: POST to /api/transactions
        mockMvc.perform(post("/api/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonPayload))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.transactionId").isNotEmpty())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));
    }

    @Test
    void healthCheckShouldReturnOk() throws Exception {
        mockMvc.perform(get("/api/transactions/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("Transaction API is running"));
    }
}
