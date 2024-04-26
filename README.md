# ArangoDB Spark Datasource Demo

This demo is composed of 3 parts:

- `WriteDemo`: reads the input json files as Spark Dataframes, applies conversions to map the data to Spark data types
  and writes the records into ArangoDB collections
- `ReadDemo`: reads the ArangoDB collections created above as Spark Dataframes, specifying columns selection and records
  filters predicates or custom AQL queries
- `ReadWriteDemo`: reads the ArangoDB collections created above as Spark Dataframes, applies projections and filtering,
  writes to a new ArangoDB collection

There are demos available written in Scala & Python (using PySpark) as outlined below.

## Requirements

This demo requires:

- JDK 11
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
