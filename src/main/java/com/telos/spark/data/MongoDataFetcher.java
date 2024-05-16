package com.telos.spark.data;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNoneBlank;

import com.telos.repository.fetch.DatabaseQuery;
import com.telos.spark.conf.SparkOptions;
import com.telos.spark.conf.SparkProperties;
import java.util.Map;
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

  private static final String SELECT_TEMPLATE = "FOR ${collection} IN ${collection}";
  //  private static final String FILTER_TEMPLATE = "FILTER ${filter}";
  private static final String RETURN_PROJECTION_TEMPLATE = "RETURN {${projection}}";
  private static final String RETURN_COLLECTION_TEMPLATE = "RETURN ${collection}";

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

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
        .option(SparkOptions.Arango.TABLE, collection)
        .option(SparkOptions.Common.INFER_SCHEMA, true)
        .load();
  }

  public Dataset<Row> fetchQuery(final String query) {
    System.out.println("query --> " + query);
    return this.dataFrameReader()
        .option(SparkOptions.Arango.QUERY, query)
        .option(SparkOptions.Common.INFER_SCHEMA, true)
        .load();
  }

  public Dataset<Row> fetch(final String collection, final String filter, final String projection) {
    ObjectUtils.requireNonEmpty(collection, "Collection is required");
    if (isBlank(filter) && isBlank(projection)) {
      return this.fetchCollection(collection);
    } else {
      final String selectClause =
          StringSubstitutor.replace(SELECT_TEMPLATE, Map.of("collection", collection));
      final String filterClause = isNoneBlank(filter) ? filter : "";

      final String returnClause =
          isNoneBlank(projection)
              ? StringSubstitutor.replace(
                  RETURN_PROJECTION_TEMPLATE, Map.of("projection", projection))
              : StringSubstitutor.replace(
                  RETURN_COLLECTION_TEMPLATE, Map.of("collection", collection));

      final String query = (selectClause + " " + filterClause + " " + returnClause).trim();

      return this.fetchQuery(query);
    }
  }

  public Dataset<Row> fetch(final DatabaseQuery databaseQuery) {
    return this.fetch(
        databaseQuery.getCollection(),
        databaseQuery.getQuery(),
        String.join(" ", databaseQuery.getProjectionsList()));
  }
}
