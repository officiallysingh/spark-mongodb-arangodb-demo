package com.telos.spark.data;

import com.telos.repository.fetch.DatabaseQuery;
import com.telos.spark.context.Context;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SparkSQLFetcher {

  private final Pattern VAR_PATTERN = Pattern.compile("\\$\\{([^}]*)}");

  private final SparkSession sparkSession;

  public Dataset<Row> fetch(final String query, final Context context) {
    Matcher matcher = VAR_PATTERN.matcher(query);
    Map<String, String> vars = new HashMap<>();
    // Iterate over matches and extract variable names
    while (matcher.find()) {
      String variable = matcher.group(1);
      System.out.println("Variable: " + variable);
      if (context.contains(variable)) {
        Dataset<Row> dataset = context.get(variable).get();
        dataset.createOrReplaceTempView(variable);
        vars.put(variable, variable);
      } else {
        throw new IllegalArgumentException("Dataframe with name: " + variable + " not found");
      }
    }
    if (MapUtils.isNotEmpty(vars)) {
      String finalQuery = StringSubstitutor.replace(query, vars);
      System.out.println("Spark SQL --->>" + query);
      return sparkSession.sql(finalQuery);
    } else {
      System.out.println("Spark SQL --->>" + query);
      return sparkSession.sql(query);
    }
  }

  public Dataset<Row> fetch(final DatabaseQuery databaseQuery, final Context context) {
    Matcher matcher = VAR_PATTERN.matcher(databaseQuery.getQuery());
    Map<String, String> vars = new HashMap<>();
    // Iterate over matches and extract variable names
    while (matcher.find()) {
      String variable = matcher.group(1);
      System.out.println("Variable: " + variable);
      if (context.contains(variable)) {
        Dataset<Row> dataset = context.get(variable).get();
        dataset.createOrReplaceTempView(variable);
        vars.put(variable, variable);
      } else {
        throw new IllegalArgumentException("Dataframe with name: " + variable + " not found");
      }
    }
    if (MapUtils.isNotEmpty(vars)) {
      String query = StringSubstitutor.replace(databaseQuery.getQuery(), vars);
      System.out.println("Spark SQL --->>" + query);
      return sparkSession.sql(query);
    } else {
      System.out.println("Spark SQL --->>" + databaseQuery.getQuery());
      return sparkSession.sql(databaseQuery.getQuery());
    }
  }
}
