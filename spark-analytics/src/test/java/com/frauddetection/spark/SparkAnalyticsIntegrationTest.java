package com.frauddetection.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Spark Analytics using Testcontainers Kafka
 * and local Spark (local[*] mode).
 *
 * Test flow:
 * 1. Produce sample transactions to Kafka raw-transactions topic
 * 2. Run Spark batch read from Kafka to a temp directory (simulating HDFS)
 * 3. Verify Parquet output contains expected data
 * 4. Train a simple fraud model on the output data
 */
@Testcontainers
class SparkAnalyticsIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.7.0");

    private static SparkSession spark;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @BeforeAll
    static void setUpSpark() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkAnalyticsIntegrationTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void shouldReadTransactionsFromKafkaAndWriteParquet(@TempDir Path tempDir) throws Exception {
        String bootstrapServers = kafka.getBootstrapServers();
        String outputPath = tempDir.resolve("transactions-parquet").toString();

        // Produce test transactions to Kafka
        produceTestTransactions(bootstrapServers, 20);

        // Run batch Kafka-to-output job
        KafkaToHdfsStreamingJob.runBatch(spark, bootstrapServers, outputPath);

        // Verify Parquet output
        Dataset<Row> result = spark.read().parquet(outputPath);
        long count = result.count();

        assertThat(count).isEqualTo(20);
        assertThat(result.columns()).contains("id", "userId", "amount", "merchantId", "location");

        // Verify data content
        Dataset<Row> highValue = result.filter("amount > 5000");
        assertThat(highValue.count()).isGreaterThan(0);
    }

    @Test
    void shouldTrainFraudModelOnParquetData(@TempDir Path tempDir) throws Exception {
        String bootstrapServers = kafka.getBootstrapServers();
        String dataPath = tempDir.resolve("training-data").toString();
        String modelPath = tempDir.resolve("model-output").toString();

        // Produce diverse transactions
        produceTestTransactions(bootstrapServers, 100);

        // Write batch data
        KafkaToHdfsStreamingJob.runBatch(spark, bootstrapServers, dataPath);

        // Train model
        FraudModelTrainingJob.trainAndSave(spark, dataPath, modelPath);

        // Verify model was saved
        assertThat(tempDir.resolve("model-output").toFile()).exists();
    }

    private void produceTestTransactions(String bootstrapServers, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Random random = new Random(42);
        String[] merchants = {"merch-001", "merch-002", "merch-003", "merch-004", "merch-005"};
        String[] locations = {"New York", "London", "Tokyo", "Berlin", "Sydney"};
        String[] currencies = {"USD", "EUR", "JPY", "GBP"};

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String userId = "user-" + (i % 5);
                // Make some transactions suspicious (above $5000)
                double amount = (i % 7 == 0) ? 5000 + random.nextDouble() * 5000 : random.nextDouble() * 1000;

                Map<String, Object> tx = new LinkedHashMap<>();
                tx.put("id", UUID.randomUUID().toString());
                tx.put("userId", userId);
                tx.put("amount", BigDecimal.valueOf(amount).setScale(2, java.math.RoundingMode.HALF_UP));
                tx.put("merchantId", merchants[random.nextInt(merchants.length)]);
                tx.put("location", locations[random.nextInt(locations.length)]);
                tx.put("currency", currencies[random.nextInt(currencies.length)]);
                tx.put("category", "purchase");
                tx.put("timestamp", Instant.now().toString());

                String json = objectMapper.writeValueAsString(tx);
                producer.send(new ProducerRecord<>("raw-transactions", userId, json)).get();
            }
            producer.flush();
        }
    }
}
