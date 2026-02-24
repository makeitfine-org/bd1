package com.frauddetection.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.frauddetection.flink.model.FraudAlertEvent;
import com.frauddetection.flink.model.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the Flink Fraud Detector Job using Testcontainers Kafka
 * and Flink MiniCluster (embedded via test-utils).
 *
 * Test flow:
 * 1. Produce suspicious transactions to raw-transactions topic
 * 2. Run Flink pipeline in a separate thread
 * 3. Consume fraud alerts from fraud-alerts topic
 * 4. Verify alerts were generated for high-value transactions
 */
@Testcontainers
class FlinkFraudDetectorIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.7.0");

    private static final String INPUT_TOPIC = "raw-transactions";
    private static final String OUTPUT_TOPIC = "fraud-alerts";

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void shouldDetectFraudulentTransactions() throws Exception {
        String bootstrapServers = kafka.getBootstrapServers();

        // Produce test transactions (one above $5000 threshold)
        produceTransactions(bootstrapServers, List.of(
                createTransaction("tx-1", "user-A", new BigDecimal("100.00"), "merch-1", "NYC"),
                createTransaction("tx-2", "user-A", new BigDecimal("7500.00"), "merch-2", "LA"),
                createTransaction("tx-3", "user-B", new BigDecimal("200.00"), "merch-3", "Chicago")
        ));

        // Run Flink job in a separate thread with a short window
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> flinkFuture = executor.submit(() -> {
            try {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                FlinkFraudDetectorJob.buildPipeline(
                        env,
                        bootstrapServers,
                        INPUT_TOPIC,
                        OUTPUT_TOPIC,
                        Duration.ofSeconds(5)  // Short window for testing
                );

                env.execute("Test Fraud Detection");
            } catch (Exception e) {
                if (!(e instanceof InterruptedException) && !(e.getCause() instanceof InterruptedException)) {
                    throw new RuntimeException(e);
                }
            }
        });

        // Consume fraud alerts
        List<FraudAlertEvent> alerts = new ArrayList<>();
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-alert-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    FraudAlertEvent alert = objectMapper.readValue(record.value(), FraudAlertEvent.class);
                    alerts.add(alert);
                }
                // At least one alert should be generated for the $7500 transaction
                assertThat(alerts).isNotEmpty();
            });
        }

        // Verify alerts
        assertThat(alerts).anyMatch(a ->
                "user-A".equals(a.getUserId()) &&
                a.getTransactionAmount().compareTo(new BigDecimal("7500.00")) == 0
        );

        // Cleanup
        flinkFuture.cancel(true);
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    private TransactionEvent createTransaction(String id, String userId, BigDecimal amount,
                                                String merchantId, String location) {
        return new TransactionEvent(id, userId, amount, merchantId, location, "USD", "purchase", Instant.now());
    }

    private void produceTransactions(String bootstrapServers, List<TransactionEvent> transactions) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (TransactionEvent tx : transactions) {
                String json = objectMapper.writeValueAsString(tx);
                producer.send(new ProducerRecord<>(INPUT_TOPIC, tx.getUserId(), json)).get();
            }
            producer.flush();
        }
    }
}
