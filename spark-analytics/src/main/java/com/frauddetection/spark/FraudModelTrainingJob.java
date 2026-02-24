package com.frauddetection.spark;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark MLlib batch job for training a Logistic Regression fraud prediction model
 * on historical transaction data stored in HDFS (Parquet format).
 *
 * <p>Features used for training:
 * - Transaction amount
 * - Indexed merchant ID
 * - Indexed location
 * - Indexed currency
 *
 * <p>The label is synthetically generated for demonstration:
 * transactions with amount > $5,000 are labeled as fraudulent (1.0), otherwise legitimate (0.0).
 */
public class FraudModelTrainingJob {

    private static final Logger LOG = LoggerFactory.getLogger(FraudModelTrainingJob.class);

    /**
     * Runs the model training job.
     *
     * @param hdfsInputPath  Path to historical transaction Parquet data
     * @param modelOutputPath Path to save the trained model
     */
    public static void run(String hdfsInputPath, String modelOutputPath) {
        LOG.info("Starting Fraud Model Training: input={}, modelOutput={}", hdfsInputPath, modelOutputPath);

        SparkSession spark = SparkSession.builder()
                .appName("FraudDetection-ModelTraining")
                .getOrCreate();

        trainAndSave(spark, hdfsInputPath, modelOutputPath);
    }

    /**
     * Core training logic, extracted for testability.
     */
    public static void trainAndSave(SparkSession spark, String inputPath, String modelOutputPath) {
        // Read historical data
        Dataset<Row> rawData = spark.read().parquet(inputPath);
        LOG.info("Loaded {} records from {}", rawData.count(), inputPath);

        // Feature engineering: create label (synthetic for demo)
        Dataset<Row> data = rawData
                .withColumn("label",
                        functions.when(functions.col("amount").gt(5000), 1.0)
                                .otherwise(0.0))
                .na().fill("UNKNOWN", new String[]{"merchantId", "location", "currency"})
                .na().fill(0.0, new String[]{"amount"});

        // String indexing for categorical features
        StringIndexer merchantIndexer = new StringIndexer()
                .setInputCol("merchantId")
                .setOutputCol("merchantIndex")
                .setHandleInvalid("keep");

        StringIndexer locationIndexer = new StringIndexer()
                .setInputCol("location")
                .setOutputCol("locationIndex")
                .setHandleInvalid("keep");

        StringIndexer currencyIndexer = new StringIndexer()
                .setInputCol("currency")
                .setOutputCol("currencyIndex")
                .setHandleInvalid("keep");

        // Vector assembler
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"amount", "merchantIndex", "locationIndex", "currencyIndex"})
                .setOutputCol("features");

        // Logistic Regression
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(100)
                .setRegParam(0.01)
                .setElasticNetParam(0.8)
                .setFeaturesCol("features")
                .setLabelCol("label");

        // Build pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{merchantIndexer, locationIndexer, currencyIndexer, assembler, lr});

        // Split data
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.8, 0.2}, 42);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        LOG.info("Training: {} records, Test: {} records", trainingData.count(), testData.count());

        // Train model
        PipelineModel model = pipeline.fit(trainingData);

        // Evaluate
        Dataset<Row> predictions = model.transform(testData);
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol("label")
                .setRawPredictionCol("rawPrediction")
                .setMetricName("areaUnderROC");

        double auc = evaluator.evaluate(predictions);
        LOG.info("Model AUC-ROC: {}", auc);

        // Show sample predictions
        predictions.select("userId", "amount", "label", "prediction", "probability")
                .show(10, false);

        // Save model
        try {
            model.write().overwrite().save(modelOutputPath);
            LOG.info("Model saved to: {}", modelOutputPath);
        } catch (Exception e) {
            LOG.error("Failed to save model", e);
            throw new RuntimeException(e);
        }
    }
}
