package com.telos.spark;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Schemas {

  public static class Common {
    public static final String CUSTOMER_ID = "customer_id";
    public static final String CUSTOMER_NAME = "customer_name";
    public static final String PRODUCT_ID = "product_id";
    public static final String PRODUCT_NAME = "product_name";
  }

  public static class Arango {
    public static StructField _ID = DataTypes.createStructField("_id", DataTypes.StringType, false);
    public static StructField _KEY =
        DataTypes.createStructField("_key", DataTypes.StringType, false);
    public static StructField _REV =
        DataTypes.createStructField("_rev", DataTypes.StringType, false);

    public static final StructField[] ARANGO_SYSTEM_FIELDS =
        new StructField[] {
          //        _ID,
          _KEY,
          //        _REV
        };
  }

  public static class RetailCustomer {
    public static StructField NAME =
        DataTypes.createStructField("name", DataTypes.StringType, true);

    public static StructType SCHEMA =
        DataTypes.createStructType(ArrayUtils.addAll(Arango.ARANGO_SYSTEM_FIELDS, NAME));
  }

  public static class Product {
    public static StructField NAME =
        DataTypes.createStructField("name", DataTypes.StringType, true);

    public static StructType SCHEMA =
        DataTypes.createStructType(ArrayUtils.addAll(Arango.ARANGO_SYSTEM_FIELDS, NAME));
  }

  public static class Feature {
    public static StructField ID = DataTypes.createStructField("_id", DataTypes.StringType, false);
    public static StructField RECORD_KEY =
        DataTypes.createStructField("record_key", DataTypes.StringType, false);
    public static StructField ONE = DataTypes.createStructField("1", DataTypes.IntegerType, false);
    public static StructField TWO = DataTypes.createStructField("2", DataTypes.IntegerType, false);
    public static StructField FEATURE_ID =
        DataTypes.createStructField("feature_id", DataTypes.IntegerType, false);
    public static StructField FEATURE_VALUE =
        DataTypes.createStructField("feature_value", DataTypes.IntegerType, false);
    public static StructField QUANTITY =
        DataTypes.createStructField("quantity", DataTypes.StringType, true);
    public static StructField WINDOW_TYPE =
        DataTypes.createStructField("window_type", DataTypes.StringType, false);
    public static StructField TIMESTAMP =
        DataTypes.createStructField("timestamp", DataTypes.TimestampType, false);

    public static StructType SCHEMA =
        DataTypes.createStructType(
            new StructField[] {
              //          ID,
              //          RECORD_KEY,
              ONE, TWO, FEATURE_ID, FEATURE_VALUE,
              //          QUANTITY,
              //          WINDOW_TYPE,
              //          TIMESTAMP
            });
  }

  public static class Inference {
    public static StructField ID = DataTypes.createStructField("_id", DataTypes.StringType, false);
    public static StructField RECORD_KEY =
        DataTypes.createStructField("record_key", DataTypes.StringType, false);
    public static StructField ONE = DataTypes.createStructField("1", DataTypes.IntegerType, false);
    public static StructField TWO = DataTypes.createStructField("2", DataTypes.IntegerType, false);
    public static StructField INFERENCE_ID =
        DataTypes.createStructField("inference_id", DataTypes.IntegerType, false);
    public static StructField INFERENCE_VALUE =
        DataTypes.createStructField("inference_value", DataTypes.IntegerType, false);
    public static StructField QUANTITY =
        DataTypes.createStructField("quantity", DataTypes.StringType, true);
    public static StructField TIMESTAMP =
        DataTypes.createStructField("timestamp", DataTypes.TimestampType, false);

    public static StructType SCHEMA =
        DataTypes.createStructType(
            new StructField[] {
              //          ID,
              //          RECORD_KEY,
              ONE, TWO, INFERENCE_ID, INFERENCE_VALUE,
              //          QUANTITY,
              //          TIMESTAMP
            });
  }

  public static class Label {
    public static StructField ID = DataTypes.createStructField("_id", DataTypes.StringType, false);
    public static StructField RECORD_KEY =
        DataTypes.createStructField("record_key", DataTypes.StringType, false);
    public static StructField ONE = DataTypes.createStructField("1", DataTypes.IntegerType, false);
    public static StructField TWO = DataTypes.createStructField("2", DataTypes.IntegerType, false);
    public static StructField LABEL_ID =
        DataTypes.createStructField("label_id", DataTypes.IntegerType, false);
    public static StructField LABEL_VALUE =
        DataTypes.createStructField("label_value", DataTypes.IntegerType, false);
    public static StructField QUANTITY =
        DataTypes.createStructField("quantity", DataTypes.StringType, true);
    public static StructField TIMESTAMP =
        DataTypes.createStructField("timestamp", DataTypes.TimestampType, false);

    public static StructType SCHEMA =
        DataTypes.createStructType(
            new StructField[] {
              //          ID,
              //          RECORD_KEY,
              ONE, TWO, LABEL_ID, LABEL_VALUE,
              //          QUANTITY,
              //          TIMESTAMP
            });
  }
}
