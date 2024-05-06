package com.telos.spark.data;

import static com.telos.spark.Schemas.Common.CUSTOMER_ID;
import static com.telos.spark.Schemas.Common.PRODUCT_ID;

import com.telos.spark.Schemas;
import com.telos.spark.conf.SparkOptions;
import com.telos.spark.conf.SparkProperties;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TransactionDataframeLoader {

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  //  ------------- Features -------------
  public Dataset<Row> featuresDataframe() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Mongo.FORMAT)
        .option(SparkOptions.Mongo.READ_CONNECTION_URI, this.sparkProperties.getMongo().getUrl())
        .option(
            SparkOptions.Mongo.DATABASE, this.sparkProperties.getMongo().getFeature().getDatabase())
        .option(
            SparkOptions.Mongo.COLLECTION,
            this.sparkProperties.getMongo().getFeature().getCollection())
        .schema(Schemas.Feature.SCHEMA)
        .load()
        .withColumnRenamed(Schemas.Feature.ONE.name(), CUSTOMER_ID)
        .withColumnRenamed(Schemas.Feature.TWO.name(), PRODUCT_ID)
        .drop(Schemas.Feature.ONE.name(), Schemas.Feature.TWO.name());
  }

  //  ------------- Inferences -------------

  public Dataset<Row> inferencesDataframe() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Mongo.FORMAT)
        .option(SparkOptions.Mongo.READ_CONNECTION_URI, this.sparkProperties.getMongo().getUrl())
        .option(
            SparkOptions.Mongo.DATABASE,
            this.sparkProperties.getMongo().getInference().getDatabase())
        .option(
            SparkOptions.Mongo.COLLECTION,
            this.sparkProperties.getMongo().getInference().getCollection())
        .schema(Schemas.Inference.SCHEMA)
        .load()
        .withColumnRenamed(Schemas.Inference.ONE.name(), CUSTOMER_ID)
        .withColumnRenamed(Schemas.Inference.TWO.name(), PRODUCT_ID)
        .drop(Schemas.Inference.ONE.name(), Schemas.Inference.TWO.name());
  }

  //  ------------- Labels -------------
  public Dataset<Row> labelsDataframe() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Mongo.FORMAT)
        .option(SparkOptions.Mongo.READ_CONNECTION_URI, this.sparkProperties.getMongo().getUrl())
        .option(
            SparkOptions.Mongo.DATABASE, this.sparkProperties.getMongo().getLabel().getDatabase())
        .option(
            SparkOptions.Mongo.COLLECTION,
            this.sparkProperties.getMongo().getLabel().getCollection())
        .schema(Schemas.Label.SCHEMA)
        .load()
        .withColumnRenamed(Schemas.Label.ONE.name(), CUSTOMER_ID)
        .withColumnRenamed(Schemas.Label.TWO.name(), PRODUCT_ID)
        .drop(Schemas.Label.ONE.name(), Schemas.Label.TWO.name());
  }
}
