package com.github.schm1tz1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.retry.annotation.EnableRetry;

/** Spring Boot entrypoint. Kafka Streams lifecycle is managed by StreamsBuilderFactoryBean. */
@SpringBootApplication
@EnableKafkaStreams
@EnableRetry
public class KStreamsSpringBootApp {
  public static void main(String[] args) {
    SpringApplication.run(KStreamsSpringBootApp.class, args);
  }
}
