package com.telos.spark.ml;

import static com.telos.spark.Schemas.Common.CUSTOMER_ID;
import static com.telos.spark.Schemas.Common.CUSTOMER_NAME;
import static com.telos.spark.Schemas.Common.PRODUCT_ID;
import static com.telos.spark.Schemas.Common.PRODUCT_NAME;
import static com.telos.spark.conf.SparkOptions.Join.LEFT;
import static org.apache.spark.sql.functions.*;

import com.telos.spark.conf.SparkOptions;
import com.telos.spark.data.KnowledgeDataframeLoader;
import com.telos.spark.data.TransactionframeLoader;
import java.util.Arrays;
import java.util.Comparator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TelosMLExecutor {

  private final KnowledgeDataframeLoader arangoDataframeLoader;

  private final TransactionframeLoader mongoDataframeLoader;

  public void execute() {
    log.info("Loading Retail Customers data from ArangoDB...");
    Dataset<Row> retailCustomersDf = this.arangoDataframeLoader.retailCustomersDataframe();
    retailCustomersDf.printSchema();
    retailCustomersDf.show(5, false);

    log.info("Loading Products data from ArangoDB...");
    Dataset<Row> productsDf = this.arangoDataframeLoader.productsDataframe();
    productsDf.printSchema();
    productsDf.show(5, false);

    log.info("Loading Features data from MongoDB...");
    Dataset<Row> featuresDf = this.mongoDataframeLoader.featuresDataframe();
    featuresDf =
        featuresDf
            .groupBy(CUSTOMER_ID, PRODUCT_ID)
            .pivot(concat(lit("feature_"), featuresDf.col("feature_id")))
            .agg(functions.first("feature_value"));
    featuresDf.printSchema();
    featuresDf.show(5, false);

    Dataset<Row> customerFeaturesDf = retailCustomersDf.join(featuresDf, CUSTOMER_ID, LEFT);
    customerFeaturesDf.show(10, false);
    Dataset<Row> customerProductFeaturesDf = customerFeaturesDf.join(productsDf, PRODUCT_ID, LEFT);
    customerProductFeaturesDf.show(10, false);

    log.info("Loading Inferences data from MongoDB...");
    Dataset<Row> inferencesDf = this.mongoDataframeLoader.inferencesDataframe();
    inferencesDf =
        inferencesDf
            .groupBy(CUSTOMER_ID, PRODUCT_ID)
            .pivot(concat(lit("inference_"), inferencesDf.col("inference_id")))
            .agg(functions.first("inference_value"));
    inferencesDf.printSchema();
    inferencesDf.show(5, false);

    log.info("Loading Labels data from MongoDB...");
    Dataset<Row> labelsDf = this.mongoDataframeLoader.labelsDataframe();
    labelsDf =
        labelsDf
            .groupBy(CUSTOMER_ID, PRODUCT_ID)
            .pivot(concat(lit("label_"), labelsDf.col("label_id")))
            .agg(functions.first("label_value"));
    labelsDf.printSchema();
    labelsDf.show(5, false);

    // ----------- Joins -----------
    Dataset<Row> customerProductFeatureInferenceDf =
        customerProductFeaturesDf
            .join(
                inferencesDf,
                customerProductFeaturesDf
                    .col(CUSTOMER_ID)
                    .equalTo(inferencesDf.col(CUSTOMER_ID))
                    .and(
                        customerProductFeaturesDf
                            .col(PRODUCT_ID)
                            .equalTo(inferencesDf.col(PRODUCT_ID))),
                LEFT)
            .drop(inferencesDf.col(CUSTOMER_ID), inferencesDf.col(PRODUCT_ID));
    customerProductFeatureInferenceDf.show(50, false);

    Dataset<Row> resultDf =
        customerProductFeatureInferenceDf
            .join(
                labelsDf,
                customerProductFeatureInferenceDf
                    .col(CUSTOMER_ID)
                    .equalTo(labelsDf.col(CUSTOMER_ID))
                    .and(
                        customerProductFeatureInferenceDf
                            .col(PRODUCT_ID)
                            .equalTo(labelsDf.col(PRODUCT_ID))),
                LEFT)
            .drop(labelsDf.col(CUSTOMER_ID), labelsDf.col(PRODUCT_ID));

    final String[] columns = resultDf.columns();

    final Comparator<String> COLUMN_NAME_COMPARATOR =
        Comparator.comparingInt(
            col -> {
              switch (col) {
                case CUSTOMER_ID:
                  return 1;
                case CUSTOMER_NAME:
                  return 2;
                case PRODUCT_ID:
                  return 3;
                case PRODUCT_NAME:
                  return 4;
                default:
                  return 5;
              }
            });

    final String[] sortedColumns =
        Arrays.stream(columns).sorted(COLUMN_NAME_COMPARATOR).toArray(String[]::new);

    resultDf = resultDf.selectExpr(sortedColumns);
    resultDf.show(50, false);

    // Writing the DataFrame to a Parquet file
    resultDf
        .write()
        .mode(SaveMode.Overwrite) // Specify the save mode: overwrite, append, ignore, error,
        // errorifexists
        .option(SparkOptions.Common.HEADER, true) // Include header
        .parquet("spark-mongodb-arangodb-demo/export/output.parquet");
  }
}
