package com.frauddetection.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * Spark Structured Streaming job that consumes raw transaction events from Kafka
 * and writes them as Parquet files to HDFS for long-term storage and batch analytics.
 */
public class KafkaToHdfsStreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToHdfsStreamingJob.class);

    private static final String RAW_TRANSACTIONS_TOPIC = "raw-transactions";

    /**
     * Transaction JSON schema.
     */
    private static final StructType TRANSACTION_SCHEMA = new StructType()
            .add("id", DataTypes.StringType)
            .add("userId", DataTypes.StringType)
            .add("amount", DataTypes.DoubleType)
            .add("merchantId", DataTypes.StringType)
            .add("location", DataTypes.StringType)
            .add("currency", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("timestamp", DataTypes.StringType);

    /**
     * Runs the Kafka → HDFS streaming job.
     *
     * @param kafkaBootstrap Kafka bootstrap servers
     * @param hdfsOutputPath HDFS path for Parquet output
     */
    public static void run(String kafkaBootstrap, String hdfsOutputPath) throws TimeoutException {
        LOG.info("Starting Kafka-to-HDFS streaming job: kafka={}, output={}", kafkaBootstrap, hdfsOutputPath);

        SparkSession spark = SparkSession.builder()
                .appName("FraudDetection-KafkaToHDFS")
                .getOrCreate();

        // Read from Kafka
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrap)
                .option("subscribe", RAW_TRANSACTIONS_TOPIC)
                .option("startingOffsets", "earliest")
                .load();

        // Parse JSON from Kafka value
        Dataset<Row> transactions = kafkaStream
                .selectExpr("CAST(value AS STRING) as json_value")
                .select(functions.from_json(functions.col("json_value"), TRANSACTION_SCHEMA).as("data"))
                .select("data.*")
                .withColumn("processing_time", functions.current_timestamp())
                .withColumn("date_partition", functions.date_format(functions.current_timestamp(), "yyyy-MM-dd"));

        // Write to HDFS as Parquet, partitioned by date
        StreamingQuery query = transactions.writeStream()
                .format("parquet")
                .option("path", hdfsOutputPath)
                .option("checkpointLocation", hdfsOutputPath + "/_checkpoint")
                .partitionBy("date_partition")
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .start();

        LOG.info("Streaming query started: {}", query.name());
        query.awaitTermination();
    }

    /**
     * Runs a one-shot batch read from Kafka and writes to a local/HDFS path.
     * Useful for testing.
     */
    public static void runBatch(SparkSession spark, String kafkaBootstrap, String outputPath) {
        LOG.info("Running batch Kafka-to-output: kafka={}, output={}", kafkaBootstrap, outputPath);

        Dataset<Row> kafkaBatch = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrap)
                .option("subscribe", RAW_TRANSACTIONS_TOPIC)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load();

        Dataset<Row> transactions = kafkaBatch
                .selectExpr("CAST(value AS STRING) as json_value")
                .select(functions.from_json(functions.col("json_value"), TRANSACTION_SCHEMA).as("data"))
                .select("data.*");

        transactions.write()
                .mode("overwrite")
                .parquet(outputPath);

        LOG.info("Batch write complete to: {}", outputPath);
    }
}
