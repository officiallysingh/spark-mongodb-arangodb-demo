package com.telos.spark.data;

import com.telos.spark.conf.SparkOptions;
import com.telos.spark.conf.SparkProperties;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MongoDataframeLoader {

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  //  ------------- Features -------------
  private static StructType featuresSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          //          DataTypes.createStructField("_id", DataTypes.StringType, false),
          //          DataTypes.createStructField("record_key", DataTypes.StringType, false),
          DataTypes.createStructField("1", DataTypes.IntegerType, false),
          DataTypes.createStructField("2", DataTypes.IntegerType, false),
          DataTypes.createStructField("feature_id", DataTypes.IntegerType, false),
          DataTypes.createStructField("feature_value", DataTypes.IntegerType, false),
          //          DataTypes.createStructField("quantity", DataTypes.StringType, true),
          //          DataTypes.createStructField("window_type", DataTypes.StringType, false),
          //          DataTypes.createStructField("timestamp", DataTypes.TimestampType, false)
        });
  }

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
        .schema(featuresSchema())
        .load()
        .withColumnRenamed("1", "customer_id")
        .withColumnRenamed("2", "product_id")
        .drop("1", "2");
  }

  //  ------------- Inferences -------------
  private static StructType inferencesSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          //          DataTypes.createStructField("_id", DataTypes.StringType, false),
          //          DataTypes.createStructField("record_key", DataTypes.StringType, false),
          DataTypes.createStructField("1", DataTypes.IntegerType, false),
          DataTypes.createStructField("2", DataTypes.IntegerType, false),
          DataTypes.createStructField("inference_id", DataTypes.IntegerType, false),
          DataTypes.createStructField("inference_value", DataTypes.IntegerType, false),
          //          DataTypes.createStructField("quantity", DataTypes.StringType, true),
          //          DataTypes.createStructField("timestamp", DataTypes.TimestampType, false)
        });
  }

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
        .schema(inferencesSchema())
        .load()
        .withColumnRenamed("1", "customer_id")
        .withColumnRenamed("2", "product_id")
        .drop("1", "2");
  }

  //  ------------- Labels -------------
  private static StructType labelsSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          //          DataTypes.createStructField("_id", DataTypes.StringType, false),
          //          DataTypes.createStructField("record_key", DataTypes.StringType, false),
          DataTypes.createStructField("1", DataTypes.IntegerType, false),
          DataTypes.createStructField("2", DataTypes.IntegerType, false),
          DataTypes.createStructField("label_id", DataTypes.IntegerType, false),
          DataTypes.createStructField("label_value", DataTypes.IntegerType, false),
          //          DataTypes.createStructField("quantity", DataTypes.StringType, true),
          //          DataTypes.createStructField("timestamp", DataTypes.TimestampType, false)
        });
  }

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
        .schema(labelsSchema())
        .load()
        .withColumnRenamed("1", "customer_id")
        .withColumnRenamed("2", "product_id")
        .drop("1", "2");
  }
}
