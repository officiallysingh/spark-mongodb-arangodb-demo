package com.telos.ml;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;

@EnableTask
@SpringBootApplication
@Slf4j
public class TelosMLTask {

  public static void main(final String[] args) {
    SpringApplication.run(TelosMLTask.class, args);
    System.out.println("TelosMLTask executed successfully .........");
  }
}
