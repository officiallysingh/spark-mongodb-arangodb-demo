package com.telos.spark.data;

import com.telos.spark.conf.SparkOptions;
import com.telos.spark.conf.SparkProperties;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MongoDataframeLoader {

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  private static final String collection = "people";

  private static final String query = "";

  public Dataset<Row> load() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Mongo.FORMAT)
        .option(
            SparkOptions.Mongo.READ_CONNECTION_URI,
            this.sparkProperties.getMongo().getConnection().getUrl())
        .option(
            SparkOptions.Mongo.DATABASE,
            this.sparkProperties.getMongo().getConnection().getDatabase())
        .option(SparkOptions.Mongo.COLLECTION, collection)
        .load();
  }
}
