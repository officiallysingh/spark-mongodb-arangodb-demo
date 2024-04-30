package com.telos.spark.data;

import com.telos.spark.conf.SparkOptions;
import com.telos.spark.conf.SparkProperties;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KnowledgeDataframeLoader {

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  private static final String COLLECTION_PRODUCT = "product";

  private static final String COLLECTION_RETAIL_CUSTOMER = "retail_customer";

  private static final StructField[] ARANGO_SYSTEM_FIELDS =
      new StructField[] {
        //        DataTypes.createStructField("_id", DataTypes.StringType, false),
        DataTypes.createStructField("_key", DataTypes.StringType, false),
        //        DataTypes.createStructField("_rev", DataTypes.StringType, false)
      };

  //  ------------- Retail Customers -------------
  private static StructType retailCustomerSchema() {
    return DataTypes.createStructType(
        ArrayUtils.addAll(
            ARANGO_SYSTEM_FIELDS, DataTypes.createStructField("name", DataTypes.StringType, true)));
  }

  public Dataset<Row> retailCustomerDataframe() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Arango.FORMAT)
        .option(SparkOptions.Arango.ENDPOINTS, this.sparkProperties.getArango().endpoints())
        .option(SparkOptions.Arango.DATABASE, this.sparkProperties.getArango().getDatabase())
        .option(SparkOptions.Arango.USERNAME, this.sparkProperties.getArango().getUsername())
        .option(SparkOptions.Arango.PASSWORD, this.sparkProperties.getArango().getPassword())
        .option(SparkOptions.Arango.SSL_ENABLED, this.sparkProperties.getArango().isSslEnabled())
        .option(SparkOptions.Arango.TABLE, COLLECTION_RETAIL_CUSTOMER)
        .schema(retailCustomerSchema())
        .load()
        .withColumnRenamed("_key", "customer_id")
        .withColumnRenamed("name", "customer_name");
  }

  //  ------------- Products -------------
  private static StructType productSchema() {
    return DataTypes.createStructType(
        ArrayUtils.addAll(
            ARANGO_SYSTEM_FIELDS, DataTypes.createStructField("name", DataTypes.StringType, true)));
  }

  public Dataset<Row> productDataframe() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Arango.FORMAT)
        .option(SparkOptions.Arango.ENDPOINTS, this.sparkProperties.getArango().endpoints())
        .option(SparkOptions.Arango.DATABASE, this.sparkProperties.getArango().getDatabase())
        .option(SparkOptions.Arango.USERNAME, this.sparkProperties.getArango().getUsername())
        .option(SparkOptions.Arango.PASSWORD, this.sparkProperties.getArango().getPassword())
        .option(SparkOptions.Arango.SSL_ENABLED, this.sparkProperties.getArango().isSslEnabled())
        .option(SparkOptions.Arango.TABLE, COLLECTION_PRODUCT)
        .schema(productSchema())
        .load()
        .withColumnRenamed("_key", "product_id")
        .withColumnRenamed("name", "product_name");
  }
}
