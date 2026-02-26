package com.frauddetection.flink;

import com.frauddetection.flink.function.FraudScoringFunction;
import com.frauddetection.flink.model.FraudAlertEvent;
import com.frauddetection.flink.model.TransactionEvent;
import com.frauddetection.flink.serialization.FraudAlertSerializationSchema;
import com.frauddetection.flink.serialization.TransactionDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Apache Flink job for real-time financial fraud detection.
 *
 * <p>Pipeline:
 * 1. Consumes transaction events from Kafka topic "raw-transactions"
 * 2. Keys by userId and applies a 1-minute tumbling window
 * 3. Applies fraud scoring rules within the FraudScoringFunction
 * 4. Publishes fraud alerts to "fraud-alerts" Kafka topic
 */
public class FlinkFraudDetectorJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkFraudDetectorJob.class);

    private static final String DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String RAW_TRANSACTIONS_TOPIC = "raw-transactions";
    private static final String FRAUD_ALERTS_TOPIC = "fraud-alerts";
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP);
        LOG.info("Starting Flink Fraud Detector Job with Kafka bootstrap: {}", kafkaBootstrap);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000); // checkpoint every 60 seconds

        buildPipeline(env, kafkaBootstrap, RAW_TRANSACTIONS_TOPIC, FRAUD_ALERTS_TOPIC, WINDOW_SIZE);

        env.execute("Fraud Detection Pipeline");
    }

    /**
     * Builds the Flink pipeline. Extracted for testability.
     */
    public static DataStream<FraudAlertEvent> buildPipeline(
            StreamExecutionEnvironment env,
            String kafkaBootstrap,
            String inputTopic,
            String outputTopic,
            Duration windowSize) {

        // Kafka Source
        KafkaSource<TransactionEvent> kafkaSource = KafkaSource.<TransactionEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(inputTopic)
                .setGroupId("flink-fraud-detector")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionDeserializationSchema())
                .build();

        DataStream<TransactionEvent> transactionStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Transaction Source"
        );

        // Fraud Detection Pipeline
        DataStream<FraudAlertEvent> fraudAlerts = transactionStream
                .keyBy(TransactionEvent::getUserId)
                .process(new FraudScoringFunction())
                .name("Fraud Scoring");

        // Kafka Sink for fraud alerts
        KafkaSink<FraudAlertEvent> kafkaSink = KafkaSink.<FraudAlertEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new FraudAlertSerializationSchema())
                                .build()
                )
                .build();

        fraudAlerts.sinkTo(kafkaSink).name("Kafka Fraud Alert Sink");

        LOG.info("Pipeline built: {} → keyBy(userId) → FraudScoring → {}", inputTopic, outputTopic);

        return fraudAlerts;
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
