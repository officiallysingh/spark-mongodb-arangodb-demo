package com.telos.spark.data;

import static com.telos.spark.Schemas.Common.CUSTOMER_ID;
import static com.telos.spark.Schemas.Common.CUSTOMER_NAME;
import static com.telos.spark.Schemas.Common.PRODUCT_ID;
import static com.telos.spark.Schemas.Common.PRODUCT_NAME;

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
public class KnowledgeDataframeLoader {

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  private static final String COLLECTION_RETAIL_CUSTOMER = "retail_customer";

  private static final String COLLECTION_PRODUCT = "product";

  //  ------------- Retail Customers -------------
  public Dataset<Row> retailCustomersDataframe() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Arango.FORMAT)
        .option(SparkOptions.Arango.ENDPOINTS, this.sparkProperties.getArango().endpoints())
        .option(SparkOptions.Arango.DATABASE, this.sparkProperties.getArango().getDatabase())
        .option(SparkOptions.Arango.USERNAME, this.sparkProperties.getArango().getUsername())
        .option(SparkOptions.Arango.PASSWORD, this.sparkProperties.getArango().getPassword())
        .option(SparkOptions.Arango.SSL_ENABLED, this.sparkProperties.getArango().isSslEnabled())
        .option(SparkOptions.Arango.TABLE, COLLECTION_RETAIL_CUSTOMER)
        .schema(Schemas.RetailCustomer.SCHEMA)
        .load()
        .withColumnRenamed(Schemas.Arango._KEY.name(), CUSTOMER_ID)
        .withColumnRenamed(Schemas.RetailCustomer.NAME.name(), CUSTOMER_NAME);
  }

  //  ------------- Products -------------
  public Dataset<Row> productsDataframe() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Arango.FORMAT)
        .option(SparkOptions.Arango.ENDPOINTS, this.sparkProperties.getArango().endpoints())
        .option(SparkOptions.Arango.DATABASE, this.sparkProperties.getArango().getDatabase())
        .option(SparkOptions.Arango.USERNAME, this.sparkProperties.getArango().getUsername())
        .option(SparkOptions.Arango.PASSWORD, this.sparkProperties.getArango().getPassword())
        .option(SparkOptions.Arango.SSL_ENABLED, this.sparkProperties.getArango().isSslEnabled())
        .option(SparkOptions.Arango.TABLE, COLLECTION_PRODUCT)
        .schema(Schemas.Product.SCHEMA)
        .load()
        .withColumnRenamed(Schemas.Arango._KEY.name(), PRODUCT_ID)
        .withColumnRenamed(Schemas.Product.NAME.name(), PRODUCT_NAME);
  }
}
