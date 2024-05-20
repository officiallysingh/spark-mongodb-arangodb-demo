# ArangoDB & MongoDB Spark Datasource Demo

Run [**`TelosMLTask`**](src/main/java/com/telos/spark/TelosMLTask.java) as Spring boot application.

> [!IMPORTANT]
> Set VM argument `--add-exports java.base/sun.nio.ch=ALL-UNNAMED`

## Requirements

This demo requires:

- JDK 17
- `maven`
- `docker`

## Prepare the environment
### Local ArangoDB installation
* use `endpoints` as `localhost:8529`.
* default user is `root` so no need to specify unless its different.
* default password is blank unless set to some other value.
* DB WebUI is accessible at [**http://localhost:8529/_db/test_db/_admin/aardvark/index.html#login**](http://localhost:8529/_db/test_db/_admin/aardvark/index.html#login)
* Run the Demo class's as Java main methods.

### Docker & Spark cluster
Set environment variables:

```shell
export ARANGO_SPARK_VERSION=1.6.0
```

Start ArangoDB cluster with docker:

```shell
STARTER_MODE=cluster ./docker/start_db.sh
```

The deployed cluster will be accessible at [http://172.28.0.1:8529](http://172.28.0.1:8529) with username `root` and
password `test`.

Start Spark cluster:

```shell
./docker/start_spark_3.4.sh 
```

## Run embedded

Test the Spark application in embedded mode:

```shell
mvn test
```

Test the Spark application against ArangoDB Oasis deployment:

```shell
mvn \
  -Dpassword=<root-password> \
  -Dendpoints=<endpoint> \
  -Dssl.enabled=true \
  -Dssl.cert.value=<base64-encoded-cert> \
  test
```

## Submit to Spark cluster

Package the application:

```shell
mvn -DskipTests=true package
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd):/demo \
  -v $(pwd)/docker/.ivy2:/opt/bitnami/spark/.ivy2 \
  --network arangodb \
  docker.io/bitnami/spark:3.4.0 \
  ./bin/spark-submit --master spark://spark-master:7077 \
    --packages="com.arangodb:arangodb-spark-datasource-3.4_2.12:$ARANGO_SPARK_VERSION" \
    --class Demo /demo/target/demo-$ARANGO_SPARK_VERSION.jar
```

## Fetch Conditions
### ArangoDB
* Collection: Name of collection e.g. `retail_customer`
* Query: Arrango Query (AQL format) `FILTER ${collection}._key IN ["1000", "1001", "1002"]` or `LIMIT 100`
* Projections: Comma seperated list of columns to return `customer_id: ${collection}._key` (Multiple allowed)
* ResultSet: Unique Name of DataFrame to store result e.g. `retailCustomersDf`, it can be referred in subsequent SparkSQL queries
* DataSourceType: `DataSourceType.DATA_SOURCE_TYPE_ARANGO`

### MongoDB
Spark MongoDB connector does not support queries and projections directly, 
but they are pushed with additional SparkSQL query on the resultset of the collection fetched.
* Collection: Name of collection e.g. `customer_product`
* Query: Should be in SparkSQL format e.g. `\`68\` in (SELECT customer_id from ${retailCustomersDf}) and `62` in (SELECT product_id from ${retailProductsDf})`
* Projections: Comma seperated list of columns to return `\`68\` as customer_id, \`62\` as product_id, quantity, feature_id, feature_value`.
* ResultSet: Unique Name of DataFrame to store result e.g. `retailCustomersDf`, it can be referred in subsequent SparkSQL queries
* DataSourceType: `DataSourceType.DATA_SOURCE_TYPE_MONGO`

### Spark
Spark SQL can be executed on Spark Datasets referred to as ${resultset name}, already available in context.
* Collection: Not required as of now.
* Query: Should be valid Spark SQL query e.g. ``
* Projections: Not required as of now.
* ResultSet: Unique Name of DataFrame to store result e.g. `retailCustomersDf`, it can be referred in subsequent SparkSQL queries
* DataSourceType: `DataSourceType.DATA_SOURCE_TYPE_MONGO`