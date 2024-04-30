package com.telos.spark.ml;

import static com.telos.spark.Schemas.Common.CUSTOMER_ID;
import static com.telos.spark.Schemas.Common.PRODUCT_ID;
import static com.telos.spark.conf.SparkOptions.Join.LEFT;

import com.telos.spark.Schemas;
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
    Dataset<Row> retailCustomersDf = this.arangoDataframeLoader.retailCustomersDataframe();
    retailCustomersDf.printSchema();
    retailCustomersDf.show(5, false);

    log.info("Loading Products data from ArangoDB...");
    Dataset<Row> productsDf = this.arangoDataframeLoader.productsDataframe();
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
                    .col(CUSTOMER_ID)
                    .equalTo(inferencesDf.col(CUSTOMER_ID))
                    .and(featuresDf.col(PRODUCT_ID).equalTo(inferencesDf.col(PRODUCT_ID))),
                LEFT)
            .select(
                featuresDf.col(CUSTOMER_ID),
                featuresDf.col(PRODUCT_ID),
                featuresDf.col(Schemas.Feature.FEATURE_ID.name()),
                featuresDf.col(Schemas.Feature.FEATURE_VALUE.name()),
                inferencesDf.col(Schemas.Inference.INFERENCE_ID.name()),
                inferencesDf.col(Schemas.Inference.INFERENCE_VALUE.name()));
    //    featuresInferenceJoined.show(50, false);

    // Then join the result with labelsDf
    Dataset<Row> result =
        featuresInferenceJoined
            .join(
                labelsDf,
                featuresInferenceJoined
                    .col(CUSTOMER_ID)
                    .equalTo(labelsDf.col(CUSTOMER_ID))
                    .and(featuresInferenceJoined.col(PRODUCT_ID).equalTo(labelsDf.col(PRODUCT_ID))),
                LEFT)
            .select(
                featuresInferenceJoined.col(CUSTOMER_ID),
                featuresInferenceJoined.col(PRODUCT_ID),
                featuresInferenceJoined.col(Schemas.Feature.FEATURE_ID.name()),
                featuresInferenceJoined.col(Schemas.Feature.FEATURE_VALUE.name()),
                featuresInferenceJoined.col(Schemas.Inference.INFERENCE_ID.name()),
                featuresInferenceJoined.col(Schemas.Inference.INFERENCE_VALUE.name()),
                labelsDf.col(Schemas.Label.LABEL_ID.name()),
                labelsDf.col(Schemas.Label.LABEL_VALUE.name()));

    // Show the result or save it to a file or database
    //    result.show(1000, false);

    result = result.join(retailCustomersDf, CUSTOMER_ID, LEFT);
    //    result.show(1000, false);

    result =
        result
            .join(productsDf, "product_id", LEFT)
            .select(
                result.col(CUSTOMER_ID),
                result.col("customer_name"),
                result.col(PRODUCT_ID),
                productsDf.col("product_name"),
                result.col(Schemas.Feature.FEATURE_ID.name()),
                result.col(Schemas.Feature.FEATURE_VALUE.name()),
                result.col(Schemas.Inference.INFERENCE_ID.name()),
                result.col(Schemas.Inference.INFERENCE_VALUE.name()),
                result.col(Schemas.Label.LABEL_ID.name()),
                result.col(Schemas.Label.LABEL_VALUE.name()));
    ;
    result.show(1000, false);
  }
}
