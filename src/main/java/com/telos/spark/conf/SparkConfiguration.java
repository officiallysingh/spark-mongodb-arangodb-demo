package com.telos.spark.conf;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(value = {SparkProperties.class})
@RequiredArgsConstructor
class SparkConfiguration {

  private final SparkProperties sparkProperties;

  @Bean(name = "sparkSession", destroyMethod = "stop")
  SparkSession sparkSession() {
    return SparkSession.builder()
        .appName(this.sparkProperties.getAppName())
        .master(this.sparkProperties.getMaster())
        .config(SparkOptions.Executor.INSTANCES, this.sparkProperties.getExecutor().getInstances())
        .getOrCreate();
  }
}
