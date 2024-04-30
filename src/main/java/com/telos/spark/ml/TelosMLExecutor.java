package com.telos.spark.ml;

import static org.apache.spark.sql.functions.*;

import com.telos.spark.data.KnowledgeDataframeLoader;
import com.telos.spark.data.MongoDataframeLoader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TelosMLExecutor {

  private final KnowledgeDataframeLoader arangoDataframeLoader;

  private final MongoDataframeLoader mongoDataframeLoader;

  public void execute() {
    log.info("Loading Retail Customers data from ArangoDB...");
    Dataset<Row> retailCustomersDf = this.arangoDataframeLoader.retailCustomerDataframe();
    retailCustomersDf.printSchema();
    retailCustomersDf.show(5, false);

    log.info("Loading Products data from ArangoDB...");
    Dataset<Row> productsDf = this.arangoDataframeLoader.productDataframe();
    productsDf.printSchema();
    productsDf.show(5, false);

    log.info("Loading Features data from MongoDB...");
    Dataset<Row> featuresDf = this.mongoDataframeLoader.featuresDataframe();
    featuresDf.printSchema();
    featuresDf.show(5, false);

    log.info("Loading Inferences data from MongoDB...");
    Dataset<Row> inferencesDf = this.mongoDataframeLoader.inferencesDataframe();
    inferencesDf.printSchema();
    inferencesDf.show(5, false);

    log.info("Loading Labels data from MongoDB...");
    Dataset<Row> labelsDf = this.mongoDataframeLoader.labelsDataframe();
    labelsDf.printSchema();
    labelsDf.show(5, false);

    // ----------- Joins -----------
    // First join featuresDf with inferencesDf
    Dataset<Row> featuresInferenceJoined =
        featuresDf
            .join(
                inferencesDf,
                featuresDf
                    .col("customer_id")
                    .equalTo(inferencesDf.col("customer_id"))
                    .and(featuresDf.col("product_id").equalTo(inferencesDf.col("product_id"))),
                "left")
            .select(
                featuresDf.col("customer_id"),
                featuresDf.col("product_id"),
                featuresDf.col("feature_id"),
                featuresDf.col("feature_value"),
                inferencesDf.col("inference_id"),
                inferencesDf.col("inference_value"));
    //    featuresInferenceJoined.show(50, false);

    // Then join the result with labelsDf
    Dataset<Row> result =
        featuresInferenceJoined
            .join(
                labelsDf,
                featuresInferenceJoined
                    .col("customer_id")
                    .equalTo(labelsDf.col("customer_id"))
                    .and(
                        featuresInferenceJoined
                            .col("product_id")
                            .equalTo(labelsDf.col("product_id"))),
                "left")
            .select(
                featuresInferenceJoined.col("customer_id"),
                featuresInferenceJoined.col("product_id"),
                featuresInferenceJoined.col("feature_id"),
                featuresInferenceJoined.col("feature_value"),
                featuresInferenceJoined.col("inference_id"),
                featuresInferenceJoined.col("inference_value"),
                labelsDf.col("label_id"),
                labelsDf.col("label_value"));

    // Show the result or save it to a file or database
    //    result.show(1000, false);

    result = result.join(retailCustomersDf, "customer_id", "left");
    //    result.show(1000, false);

    result =
        result
            .join(productsDf, "product_id", "inner")
            .select(
                result.col("customer_id"),
                result.col("customer_name"),
                result.col("product_id"),
                productsDf.col("product_name"),
                result.col("feature_id"),
                result.col("feature_value"),
                result.col("inference_id"),
                result.col("inference_value"),
                result.col("label_id"),
                result.col("label_value"));
    ;
    result.show(1000, false);
  }
}
