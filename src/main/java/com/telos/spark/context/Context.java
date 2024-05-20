package com.telos.spark.context;

import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Context {

  /**
   * Get a value identified by given <code>key</code>.
   *
   * @param key Resource key
   * @return Resource value, or <code>null</code> if not found
   */
  Optional<Dataset<Row>> get(final String key);

  /**
   * Bind the given resource key with the specified value
   *
   * @param key Resource key (not null)
   * @param Dataset instance. If <code>null</code>, the mapping will be removed
   * @return Previous bound instance, if any
   */
  Dataset<Row> put(final String key, final Dataset<Row> dataset);

  /**
   * Bind the given resource key with the specified value, if not already bound
   *
   * @param key Resource key (not null)
   * @param dataset Dateset instance
   * @return Previous value, or <code>null</code> if no value was bound and the new instance it's
   *     been mapped to the key
   */
  Dataset<Row> putIfAbsent(String key, Dataset<Row> dataset);

  /**
   * Removes the resource instance bound to given key
   *
   * @param key Resource key (not null)
   * @return <code>true</code> if found and removed
   */
  boolean remove(final String key);

  /**
   * Check if resource instance exists for given key
   *
   * @param key Resource key (not null)
   * @return <code>true</code> if found otherwise <code>false</code>
   */
  boolean contains(final String key);

  /** Clears all resource bindings */
  void clear();
}
