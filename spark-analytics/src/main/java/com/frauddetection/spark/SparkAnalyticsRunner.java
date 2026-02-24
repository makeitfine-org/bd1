package com.frauddetection.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for Spark analytics jobs.
 * Selects and runs a job based on the first command line argument.
 *
 * <p>Usage:
 * <pre>
 *   spark-submit --class com.frauddetection.spark.SparkAnalyticsRunner \
 *     spark-analytics.jar [kafka-to-hdfs | fraud-model-training] [args...]
 * </pre>
 */
public class SparkAnalyticsRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SparkAnalyticsRunner.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            LOG.error("Usage: SparkAnalyticsRunner <job-name> [args...]");
            LOG.error("Available jobs: kafka-to-hdfs, fraud-model-training");
            System.exit(1);
        }

        String jobName = args[0];
        LOG.info("Starting Spark job: {}", jobName);

        switch (jobName) {
            case "kafka-to-hdfs" -> {
                String kafkaBootstrap = getArgOrEnv(args, 1, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
                String hdfsPath = getArgOrEnv(args, 2, "HDFS_OUTPUT_PATH", "hdfs://namenode:9000/fraud-detection/raw-transactions");
                KafkaToHdfsStreamingJob.run(kafkaBootstrap, hdfsPath);
            }
            case "fraud-model-training" -> {
                String hdfsInputPath = getArgOrEnv(args, 1, "HDFS_INPUT_PATH", "hdfs://namenode:9000/fraud-detection/raw-transactions");
                String modelOutputPath = getArgOrEnv(args, 2, "MODEL_OUTPUT_PATH", "hdfs://namenode:9000/fraud-detection/models/fraud-lr-model");
                FraudModelTrainingJob.run(hdfsInputPath, modelOutputPath);
            }
            default -> {
                LOG.error("Unknown job: {}. Available jobs: kafka-to-hdfs, fraud-model-training", jobName);
                System.exit(1);
            }
        }
    }

    private static String getArgOrEnv(String[] args, int index, String envKey, String defaultValue) {
        if (args.length > index) {
            return args[index];
        }
        String envValue = System.getenv(envKey);
        return (envValue != null && !envValue.isBlank()) ? envValue : defaultValue;
    }
}
