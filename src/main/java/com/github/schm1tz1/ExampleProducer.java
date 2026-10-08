package com.github.schm1tz1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

/** Sends records to the pipeline input topic via KafkaTemplate, retrying failed sends. */
@Component
public class ExampleProducer {
  static final Logger logger = LoggerFactory.getLogger(ExampleProducer.class);

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String topic;

  public ExampleProducer(
      @Qualifier("kafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
      @Value("${streamsApp.inputTopic}") String topic) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
  }

  /** Blocks until the broker acks, so send failures surface here and trigger a retry. */
  @Retryable(maxAttempts = 3, backoff = @Backoff(delay = 1000, multiplier = 2))
  public void send(String key, String value) {
    Message<String> message =
        MessageBuilder.withPayload(value)
            .setHeader(KafkaHeaders.TOPIC, topic)
            .setHeader(KafkaHeaders.KEY, key)
            .build();
    logger.debug("Sending " + message);
    kafkaTemplate.send(message).join();
  }
}
