package com.telos.spark.data;

import static org.apache.commons.lang3.StringUtils.*;

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
public class ArangoDataFetcher {

  private static final String SELECT_TEMPLATE = "FOR ${collection} IN ${collection}";
  private static final String RETURN_PROJECTION_TEMPLATE = "RETURN {${projection}}";
  private static final String RETURN_COLLECTION_TEMPLATE = "RETURN ${collection}";

  private final SparkSession sparkSession;

  private final SparkProperties sparkProperties;

  private DataFrameReader dataFrameReader() {
    return this.sparkSession
        .read()
        .format(SparkOptions.Arango.FORMAT)
        .option(SparkOptions.Arango.ENDPOINTS, this.sparkProperties.getArango().endpoints())
        .option(SparkOptions.Arango.DATABASE, this.sparkProperties.getArango().getDatabase())
        .option(SparkOptions.Arango.USERNAME, this.sparkProperties.getArango().getUsername())
        .option(SparkOptions.Arango.PASSWORD, this.sparkProperties.getArango().getPassword())
        .option(SparkOptions.Arango.SSL_ENABLED, this.sparkProperties.getArango().isSslEnabled());
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
      final String filterClause =
          isNotBlank(filter)
              ? StringSubstitutor.replace(filter, Map.of("collection", collection))
              : "";

      final String returnClause;
      if (isNotBlank(projection)) {
        final String finalProjection =
            StringSubstitutor.replace(projection, Map.of("collection", collection));
        returnClause =
            StringSubstitutor.replace(
                RETURN_PROJECTION_TEMPLATE, Map.of("projection", finalProjection));
      } else {
        returnClause =
            StringSubstitutor.replace(RETURN_COLLECTION_TEMPLATE, Map.of("collection", collection));
      }

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