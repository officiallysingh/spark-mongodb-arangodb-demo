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
public class ArangoDataframeLoader {

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  private static final String table = "movies";

  private static final String query = "";

  public static StructType personSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("_id", DataTypes.StringType, false),
          DataTypes.createStructField("_key", DataTypes.StringType, false),
          DataTypes.createStructField("biography", DataTypes.StringType, true),
          DataTypes.createStructField("birthday", DataTypes.DateType, true),
          DataTypes.createStructField("birthplace", DataTypes.StringType, true),
          DataTypes.createStructField("lastModified", DataTypes.TimestampType, true),
          DataTypes.createStructField("name", DataTypes.StringType, true),
          DataTypes.createStructField("profileImageUrl", DataTypes.StringType, true)
        });
  }

  public Dataset<Row> load() {
    return this.sparkSession.read()
            .format(SparkOptions.Arango.FORMAT)
            .option(SparkOptions.Arango.ENDPOINTS, this.sparkProperties.getArango().endpoints())
            .option(SparkOptions.Arango.DATABASE, this.sparkProperties.getArango().getDatabase())
            .option(SparkOptions.Arango.USERNAME, this.sparkProperties.getArango().getUsername())
            .option(SparkOptions.Arango.PASSWORD, this.sparkProperties.getArango().getPassword())
            .option(SparkOptions.Arango.SSL_ENABLED, this.sparkProperties.getArango().isSslEnabled())
//            .option(SparkOptions.Arango.QUERY, query)
            .option(SparkOptions.Arango.TABLE, table)
            .schema(personSchema())
            .load();
  }
}
