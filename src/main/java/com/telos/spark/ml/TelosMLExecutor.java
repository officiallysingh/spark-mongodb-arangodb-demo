package com.telos.spark.ml;

import static com.telos.spark.Schemas.Common.CUSTOMER_ID;
import static com.telos.spark.Schemas.Common.CUSTOMER_NAME;
import static com.telos.spark.Schemas.Common.PRODUCT_ID;
import static com.telos.spark.Schemas.Common.PRODUCT_NAME;
import static com.telos.spark.conf.SparkOptions.Join.LEFT;
import static org.apache.spark.sql.functions.*;

import com.telos.spark.conf.SparkOptions;
import com.telos.spark.data.KnowledgeDataframeLoader;
import com.telos.spark.data.TransactionDataframeLoader;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

  private final TransactionDataframeLoader mongoDataframeLoader;

  public void execute() {
    log.info("Loading Retail Customers data from ArangoDB...");
    Dataset<Row> retailCustomersDf = this.arangoDataframeLoader.retailCustomersDataframe();
    //    retailCustomersDf.printSchema();
    //    retailCustomersDf.show(5, false);

    log.info("Loading Products data from ArangoDB...");
    Dataset<Row> productsDf = this.arangoDataframeLoader.productsDataframe();
    //    productsDf.printSchema();
    //    productsDf.show(5, false);

    Dataset<Row> customerProductsDf = retailCustomersDf.crossJoin(productsDf);
    //    customerProductsDf.printSchema();
    //    customerProductsDf.show(10000, false);

    log.info("Loading Features data from MongoDB...");
    Dataset<Row> featuresDf = this.mongoDataframeLoader.featuresDataframe();
    featuresDf =
        featuresDf
            .groupBy(CUSTOMER_ID, PRODUCT_ID)
            .pivot(concat(lit("feature_"), featuresDf.col("feature_id")))
            .agg(functions.first("feature_value"));
    //    featuresDf.printSchema();
    //    featuresDf.show(5000, false);

    log.info("Loading Inferences data from MongoDB...");
    Dataset<Row> inferencesDf = this.mongoDataframeLoader.inferencesDataframe();
    inferencesDf =
        inferencesDf
            .groupBy(CUSTOMER_ID, PRODUCT_ID)
            .pivot(concat(lit("inference_"), inferencesDf.col("inference_id")))
            .agg(functions.first("inference_value"));
    //    inferencesDf.printSchema();
    //    inferencesDf.show(5, false);

    log.info("Loading Labels data from MongoDB...");
    Dataset<Row> labelsDf = this.mongoDataframeLoader.labelsDataframe();
    labelsDf =
        labelsDf
            .groupBy(CUSTOMER_ID, PRODUCT_ID)
            .pivot(concat(lit("label_"), labelsDf.col("label_id")))
            .agg(functions.first("label_value"));
    //    labelsDf.printSchema();
    //    labelsDf.show(5, false);

    Dataset<Row> customerProductFeaturesDf =
        customerProductsDf
            .join(
                featuresDf,
                customerProductsDf
                    .col(CUSTOMER_ID)
                    .equalTo(featuresDf.col(CUSTOMER_ID))
                    .and(customerProductsDf.col(PRODUCT_ID).equalTo(featuresDf.col(PRODUCT_ID))),
                LEFT)
            .drop(featuresDf.col(CUSTOMER_ID), featuresDf.col(PRODUCT_ID));
    //    labelsDf.printSchema();
    //    customerProductFeaturesDf.show(5000, false);

    // ----------- Joins -----------
    Dataset<Row> customerProductFeatureInferencesDf =
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
    customerProductFeatureInferencesDf.show(5000, false);

    Dataset<Row> resultDf =
        customerProductFeatureInferencesDf
            .join(
                labelsDf,
                customerProductFeatureInferencesDf
                    .col(CUSTOMER_ID)
                    .equalTo(labelsDf.col(CUSTOMER_ID))
                    .and(
                        customerProductFeatureInferencesDf
                            .col(PRODUCT_ID)
                            .equalTo(labelsDf.col(PRODUCT_ID))),
                LEFT)
            .drop(labelsDf.col(CUSTOMER_ID), labelsDf.col(PRODUCT_ID));
//    resultDf.show(5000, false);

    final String[] customerProductColumns = {CUSTOMER_ID, CUSTOMER_NAME, PRODUCT_ID, PRODUCT_NAME};
    final String[] columns = resultDf.columns();

    String filterRowsWithAllColNulls =
        Arrays.stream(columns)
            .filter(colName -> !StringUtils.equalsAnyIgnoreCase(colName, customerProductColumns))
            .map(columnName -> col(columnName).isNotNull().toString())
            .collect(Collectors.joining(" OR "));

//    System.out.println("????? filterRowsWithAllColNulls -->" + filterRowsWithAllColNulls);

    resultDf = resultDf.filter(filterRowsWithAllColNulls);
    resultDf.show(5000, false);

    // Writing the DataFrame to a Parquet file
    resultDf
        .write()
        .mode(SaveMode.Overwrite) // Specify the save mode: overwrite, append, ignore, error,
        // errorifexists
        .option(SparkOptions.Common.HEADER, true) // Include header
        .parquet("spark-mongodb-arangodb-demo/export/output.parquet");
  }
}
