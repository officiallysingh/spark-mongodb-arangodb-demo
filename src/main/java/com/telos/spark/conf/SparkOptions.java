package com.telos.spark.conf;

public class SparkOptions {

    public static final String EXECUTOR_INSTANCES = "spark.executor.instances";

    public static final class Executor {

        public static final String INSTANCES = "mongodb";
    }

    public static final class Mongo {

        public static final String FORMAT = "mongodb";
        public static final String DATABASE = "database";
        public static final String COLLECTION = "collection";

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
}