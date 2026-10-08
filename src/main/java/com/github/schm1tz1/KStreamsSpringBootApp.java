package com.github.schm1tz1;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.retry.annotation.EnableRetry;

/** Spring Boot entrypoint. Kafka Streams lifecycle is managed by StreamsBuilderFactoryBean. */
@SpringBootApplication
@EnableKafkaStreams
@EnableRetry
public class KStreamsSpringBootApp {
  static final Logger logger = LoggerFactory.getLogger(KStreamsSpringBootApp.class);

  public static void main(String[] args) {
    SpringApplication.run(KStreamsSpringBootApp.class, args);
  }

  /**
   * Response to an exception escaping a stream thread. Without a handler Kafka Streams uses
   * SHUTDOWN_CLIENT: one exception stops the whole client (state ERROR, readiness DOWN).
   */
  @Bean
  StreamsBuilderFactoryBeanConfigurer uncaughtExceptionHandler(
      @Value("${streamsApp.uncaughtExceptionResponse}") StreamThreadExceptionResponse response) {
    return factoryBean ->
        factoryBean.setStreamsUncaughtExceptionHandler(
            e -> {
              logger.error("Uncaught exception in stream thread, responding with " + response, e);
              return response;
            });
  }
}
