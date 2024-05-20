package com.telos.spark.data;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNoneBlank;

import com.telos.repository.fetch.DatabaseQuery;
import com.telos.spark.conf.SparkOptions;
import com.telos.spark.conf.SparkProperties;
import java.util.HashMap;
import java.util.Map;

import com.telos.spark.context.Context;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MongoDataFetcher {

  private static final String SELECT_TEMPLATE = "SELECT ${projection} FROM ${dataframe}";
  private static final String FILTER_TEMPLATE = "WHERE ${filter}";

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  private final SparkSQLFetcher sparkSQLFetcher;

  private DataFrameReader dataFrameReader() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Mongo.FORMAT)
        .option(SparkOptions.Mongo.READ_CONNECTION_URI, this.sparkProperties.getMongo().getUrl())
        .option(
            SparkOptions.Mongo.DATABASE,
            this.sparkProperties.getMongo().getFeature().getDatabase());
  }

  public Dataset<Row> fetchCollection(final String collection) {
    return this.dataFrameReader()
        .option(SparkOptions.Mongo.COLLECTION, collection)
        .option(SparkOptions.Common.INFER_SCHEMA, true)
        .load();
  }

  public Dataset<Row> fetch(final String collection, final String filter, final String projection, final Context context) {
    ObjectUtils.requireNonEmpty(collection, "Collection is required");
    Dataset<Row> dataframe = this.fetchCollection(collection);
    if (isBlank(filter) && isBlank(projection)) {
      return dataframe;
    } else {
      Map<String, String> vars = new HashMap<>();
      vars.put("dataframe", collection + "_dataframe");
      if (isNoneBlank(projection)) {
        vars.put("projection", projection);
      } else {
        vars.put("projection", "*");
      }
      final String selectClause =
          StringSubstitutor.replace(SELECT_TEMPLATE, vars);
      final String filterClause =
          isNoneBlank(filter)
              ? StringSubstitutor.replace(FILTER_TEMPLATE, Map.of("filter", filter))
              : "";

      final String query = (selectClause + " " + filterClause).trim();
      dataframe.createOrReplaceTempView(collection + "_dataframe");
//      return this.sparkSession.sql(query);
      return this.sparkSQLFetcher.fetch(query, context);
    }
  }

  public Dataset<Row> fetch(final DatabaseQuery databaseQuery, final Context context) {
    return this.fetch(
        databaseQuery.getCollection(),
        databaseQuery.getQuery(),
        String.join(" ", databaseQuery.getProjectionsList()), context);
  }
}
