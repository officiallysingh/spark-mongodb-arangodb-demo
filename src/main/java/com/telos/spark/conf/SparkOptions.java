package com.telos.spark.conf;

public class SparkOptions {

  public static final class Executor {

    public static final String INSTANCES = "spark.executor.instances";
  }

  public static final class Common {

    public static final String HEADER = "header"; // inferSchema",

    public static final String INFER_SCHEMA = "inferSchema";
  }

  public static final class Parquet {

    public static final String FORMAT = "parquet";
    public static final String COMPRESSION = "compression";

    public static final String COMPRESSION_NONE = "none";
    public static final String COMPRESSION_UNCOMPRESSED = "uncompressed";
    public static final String COMPRESSION_SNAPPY = "snappy";
    public static final String COMPRESSION_GZIP = "gzip";
    public static final String COMPRESSION_LZO = "lzo";
    public static final String COMPRESSION_BROTLI = "brotli";
    public static final String COMPRESSION_LZ4 = "lz4";
    public static final String COMPRESSION_ZSTD = "zstd";
  }

  public static final class Mongo {

    public static final String FORMAT = "mongodb";
    public static final String DATABASE = "database";
    public static final String COLLECTION = "collection";
    public static final String QUERY = "query";
    public static final String PROJECTION = "projection";

    public static final String READ_CONNECTION_URI = "spark.mongodb.read.connection.uri";
  }

  public static final class Arango {
    public static final String FORMAT = "com.arangodb.spark";
    public static final String TABLE = "table";
    public static final String QUERY = "query";

    public static final String TABLE_TYPE_DOCUMENT = "document";
    public static final String TABLE_TYPE_EDGE = "edge";

    //  "172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549";
    public static final String ENDPOINTS = "endpoints";
    public static final String DATABASE = "database";
    public static final String USERNAME = "user";
    public static final String PASSWORD = "password";
    public static final String SSL_ENABLED = "ssl.enabled";
    public static final String SSL_CERT_VALUE = "ssl.cert.value";
  }

  public static final class Join {

    public static final String INNER = "inner";
    public static final String FULL = "full";
    public static final String FULL_OUTER = "full_outer";
    public static final String LEFT = "left";
    public static final String LEFT_OUTER = "left_outer";
    public static final String LEFT_SEMI = "left_semi";
    public static final String LEFT_ANTI = "left_anti";
    public static final String RIGHT = "right";
    public static final String RIGHT_OUTER = "right_outer";
    public static final String CROSS = "cross";
  }
}
