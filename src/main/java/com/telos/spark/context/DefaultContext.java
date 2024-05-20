package com.telos.spark.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Slf4j
public class DefaultContext implements Context {

  private final Map<String, Dataset<Row>> resources;

  private DefaultContext(final boolean threadSafe) {
    this.resources = threadSafe ? new ConcurrentHashMap<>() : new HashMap<>();
  }

  public static Context newInstance() {
    return new DefaultContext(false);
  }

  public static Context newInstance(final boolean threadSafe) {
    return new DefaultContext(threadSafe);
  }

  @Override
  public Optional<Dataset<Row>> get(final String key) {
    return Optional.ofNullable(this.resources.get(key));
  }

  @Override
  public Dataset<Row> put(final String key, final Dataset<Row> dataset) {
    Objects.requireNonNull(key, "Resource key must be not null");

    Dataset<Row> previous;
    if (dataset != null) {
      previous = this.resources.put(key, dataset);
    } else {
      previous = this.resources.remove(key);
    }
    return previous;
  }

  @Override
  public Dataset<Row> putIfAbsent(String key, Dataset<Row> dataset) {
    Objects.requireNonNull(key, "Resource key must be not null");
    if (dataset != null) {
      Dataset<Row> previous = this.resources.putIfAbsent(key, dataset);
      return previous;
    }
    return null;
  }

  @Override
  public boolean remove(final String key) {
    Objects.requireNonNull(key, "Resource key must be not null");
    Dataset<Row> removed = this.resources.remove(key);

    if (removed != null) {
      log.debug("Removed resource with key [" + key + "]");
    }

    return removed != null;
  }

  @Override
  public boolean contains(final String key) {
    return this.resources.containsKey(key);
  }

  @Override
  public void clear() {
    this.resources.clear();

    log.debug("Context cleared");
  }
}
