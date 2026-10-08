package com.github.schm1tz1;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/** Exposes the KafkaStreams state as health contributor "kafkaStreams" (used by readiness). */
@Component
public class KafkaStreamsHealthIndicator implements HealthIndicator {
  private final StreamsBuilderFactoryBean factoryBean;

  public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factoryBean) {
    this.factoryBean = factoryBean;
  }

  @Override
  public Health health() {
    KafkaStreams streams = factoryBean.getKafkaStreams();
    if (streams == null) {
      return Health.down().withDetail("state", "NOT_CREATED").build();
    }
    KafkaStreams.State state = streams.state();
    return (state.isRunningOrRebalancing() ? Health.up() : Health.down())
        .withDetail("state", state)
        .build();
  }
}
