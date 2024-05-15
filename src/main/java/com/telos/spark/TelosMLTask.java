package com.telos.spark;

import com.telos.spark.ml.TelosMLExecutor;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;

@EnableTask
@SpringBootApplication
@Slf4j
public class TelosMLTask {

  public static void main(final String[] args) {
    SpringApplication.run(TelosMLTask.class, args);
    System.out.println("TelosMLTask executed successfully .........");
  }

  @PostConstruct
  public void init() {
    log.info("Initialization ...");
  }

  @Bean
  public ApplicationRunner applicationRunner(final TelosMLExecutor telosMLExecutor) {
    return new TelosMLRunner(telosMLExecutor);
  }

  @Slf4j
  public static class TelosMLRunner implements ApplicationRunner {

    private final TelosMLExecutor telosMLExecutor;

    public TelosMLRunner(final TelosMLExecutor telosMLExecutor) {
      this.telosMLExecutor = telosMLExecutor;
    }

    @Override
    public void run(final ApplicationArguments args) throws Exception {
      log.info("Starting TelosMLTask...");
      this.telosMLExecutor.execute();
    }
  }
}
