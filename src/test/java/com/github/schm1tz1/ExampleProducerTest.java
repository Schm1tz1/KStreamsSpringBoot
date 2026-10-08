package com.github.schm1tz1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/** Verifies @Retryable proxying and the Message headers, with a mocked KafkaTemplate. */
@SpringJUnitConfig(ExampleProducerTest.Config.class)
@TestPropertySource(properties = "streamsApp.inputTopic=input-topic")
class ExampleProducerTest {
  static final Logger logger = LoggerFactory.getLogger(ExampleProducerTest.class);

  @Configuration
  @EnableRetry
  @Import(ExampleProducer.class)
  static class Config {}

  @MockitoBean(name = "kafkaTemplate")
  KafkaTemplate<String, String> kafkaTemplate;

  @Autowired ExampleProducer producer;

  @Test
  @SuppressWarnings("unchecked")
  void retriesFailedSendAndSetsHeaders() {
    AtomicInteger attempts = new AtomicInteger();
    when(kafkaTemplate.send(any(Message.class)))
        .thenAnswer(
            invocation -> {
              int attempt = attempts.incrementAndGet();
              logger.info("KafkaTemplate.send attempt {}: {}", attempt, invocation.getArgument(0));
              if (attempt == 1) {
                logger.info("Attempt 1: failing with 'broker down' to trigger @Retryable");
                return CompletableFuture.failedFuture(new RuntimeException("broker down"));
              }
              logger.info("Attempt {}: succeeding", attempt);
              return CompletableFuture.completedFuture(null);
            });

    producer.send("key1", "value1");

    ArgumentCaptor<Message<String>> captor = ArgumentCaptor.forClass(Message.class);
    verify(kafkaTemplate, times(2)).send(captor.capture());
    Message<String> sent = captor.getValue();
    logger.info("Sent after {} attempts, last message headers: {}", attempts, sent.getHeaders());
    assertEquals("value1", sent.getPayload());
    assertEquals("input-topic", sent.getHeaders().get(KafkaHeaders.TOPIC));
    assertEquals("key1", sent.getHeaders().get(KafkaHeaders.KEY));
  }
}
