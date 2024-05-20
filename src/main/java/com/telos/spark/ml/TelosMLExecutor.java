package com.telos.spark.ml;

import static org.apache.spark.sql.functions.*;

import com.telos.cortex.model.design.ModelDesignSchema;
import com.telos.repository.fetch.ModelDesignFetchRequest;
import com.telos.repository.fetch.ModelFetchCondition;
import com.telos.schema.SchemaKeyDictionary;
import com.telos.sdk.commons.SchemaCache;
import com.telos.spark.conf.SparkProperties;
import com.telos.spark.context.Context;
import com.telos.spark.context.DefaultContext;
import com.telos.spark.data.ArangoDataFetcher;
import com.telos.spark.data.MongoDataFetcher;
import com.telos.spark.data.SparkSQLFetcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TelosMLExecutor {

  private final ArangoDataFetcher arangoDataFetcher;

  private final MongoDataFetcher mongoDataFetcher;

  private final SparkSQLFetcher sparkSQLFetcher;

  private final SchemaKeyDictionary schemaKeyDictionary;

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  public TelosMLExecutor(
      final ArangoDataFetcher arangoDataFetcher,
      final MongoDataFetcher mongoDataFetcher,
      final SparkSQLFetcher sparkSQLFetcher,
      final SchemaCache schemaCache,
      final SparkSession sparkSession,
      final SparkProperties sparkProperties) {
    this.arangoDataFetcher = arangoDataFetcher;
    this.mongoDataFetcher = mongoDataFetcher;
    this.sparkSQLFetcher = sparkSQLFetcher;
    this.schemaKeyDictionary = schemaCache.initModelTrainingSchema();
    this.sparkProperties = sparkProperties;
    this.sparkSession = sparkSession;
  }

  public void execute() {
    ModelDesignSchema modelDesignSchema =
        this.schemaKeyDictionary.getModelDesignMap().get(this.sparkProperties.getModelSchemaKey());
    //    modelDesignSchema
    //        .getCriteria()
    //        .getFetchMap()
    //        .forEach(
    //            (k, v) -> {
    //              System.out.println("Key: " + k + " Value: " + v);
    //            });

    final ModelDesignFetchRequest modelDesignFetchRequest =
        modelDesignSchema.getCriteria().getFetchMap().get(this.sparkProperties.getModelKey());

    Context context = DefaultContext.newInstance(true);
    ModelFetchCondition rowFetchCondition = modelDesignFetchRequest.getRowFetchCondition();

    rowFetchCondition
        .getKnowledge()
        .getConditionList()
        .forEach(
            databaseQuery -> {
              final Dataset<Row> dataframe =
                  switch (databaseQuery.getDataSourceType()) {
                    case DATA_SOURCE_TYPE_ARANGO -> this.arangoDataFetcher.fetch(databaseQuery);
                    case DATA_SOURCE_TYPE_SPARK -> {
                      System.out.println("???");
                      yield this.mongoDataFetcher.fetch(databaseQuery, context);
                    }
                    default ->
                        throw new IllegalArgumentException(
                            "Invalid data source type: " + databaseQuery.getDataSourceType());
                  };
              context.put(databaseQuery.getResultSet(), dataframe.cache());
              dataframe.printSchema();
              dataframe.show();
            });

//    final String featuresQuery =
//        "SELECT * from customer_product WHERE `62` in (1070473321023113722, 1256622632906443508) "
//            + "and `68` in (2662899881406969352, 4276267623114056369) and feature_id in (124, 128, 129)";

    ModelFetchCondition columnFetchCondition = modelDesignFetchRequest.getColumnFetchCondition();

    columnFetchCondition
        .getFeature()
        .getConditionList()
        .forEach(
            databaseQuery -> {
              final Dataset<Row> dataframe =
                  switch (databaseQuery.getDataSourceType()) {
                    case DATA_SOURCE_TYPE_MONGO -> this.mongoDataFetcher.fetch(databaseQuery, context);
                    case DATA_SOURCE_TYPE_SPARK -> {
                      System.out.println("???");
                      yield this.sparkSQLFetcher.fetch(databaseQuery, context);
                    }
                    default ->
                        throw new IllegalArgumentException(
                            "Invalid data source type: " + databaseQuery.getDataSourceType());
                  };
              context.put(databaseQuery.getResultSet(), dataframe);
              dataframe.printSchema();
              dataframe.show();
            });

    //    featuresDf.createOrReplaceTempView("features_pivot");
    //
    //    //    featuresDf =
    //    //        this.sparkSession.sql(
    //    //            "SELECT * FROM features_pivot FOR PIVOT (concat(lit(\"feature_\"),
    //    // col(\"feature_id\")) aggregate first(\"feature_value\"))");
    //
    //    featuresDf =
    //        this.sparkSession.sql(
    //            """
    //                    SELECT * FROM features_pivot PIVOT (first(feature_value) FOR feature_id IN
    // (124, 125, 127, 128, 121, 153))
    //                    """);
    //    featuresDf.printSchema();
    //    featuresDf.show(false);
  }
}
