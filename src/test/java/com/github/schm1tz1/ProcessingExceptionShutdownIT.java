package com.github.schm1tz1;

import static com.github.schm1tz1.FailingTopology.FAIL_INPUT;
import static com.github.schm1tz1.FailingTopology.FAIL_OUTPUT;
import static com.github.schm1tz1.MonitoringIT.awaitTrue;
import static com.github.schm1tz1.TransactionTimeoutIT.read;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Exception in a processor with SHUTDOWN_CLIENT, which is also Kafka's behavior when no handler is
 * set: one exception stops the client, nothing is committed, readiness goes DOWN.
 */
@DirtiesContext
@AutoConfigureObservability(tracing = false)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "streamsApp.uncaughtExceptionResponse=SHUTDOWN_CLIENT")
@Import(FailingTopology.class)
@EmbeddedKafka(
    topics = {"input-topic", "output-topic", FAIL_INPUT, FAIL_OUTPUT},
    bootstrapServersProperty = "spring.kafka.bootstrap-servers",
    brokerProperties = {
      "transaction.state.log.replication.factor=1",
      "transaction.state.log.min.isr=1"
    })
class ProcessingExceptionShutdownIT {
  static final Logger logger = LoggerFactory.getLogger(ProcessingExceptionShutdownIT.class);

  @Autowired TestRestTemplate rest;
  @Autowired KafkaTemplate<String, String> kafkaTemplate;

  @Value("${spring.kafka.bootstrap-servers}")
  String bootstrapServers;

  @Test
  void shutdownClientStopsProcessingAndCommitsNothing() throws Exception {
    logger.info("Sending k=boom-shutdown to {}", FAIL_INPUT);
    kafkaTemplate.send(FAIL_INPUT, "k", "boom-shutdown").get();

    awaitTrue(
        "readiness DOWN (Kafka Streams state ERROR)",
        () -> {
          String readiness = rest.getForObject("/actuator/health/readiness", String.class);
          logger.trace("readiness: {}", readiness);
          return readiness.contains("\"state\":\"ERROR\"");
        });
    String readiness = rest.getForObject("/actuator/health/readiness", String.class);
    logger.info("readiness: {}", readiness);

    List<String> committed =
        read(
            bootstrapServers,
            FAIL_OUTPUT,
            "read_committed",
            "boom-shutdown",
            Duration.ofSeconds(3));
    List<String> uncommitted =
        read(
            bootstrapServers,
            FAIL_OUTPUT,
            "read_uncommitted",
            "boom-shutdown",
            Duration.ofSeconds(3));
    logger.info("read_committed sees {}, read_uncommitted sees {}", committed, uncommitted);

    assertEquals(List.of(), committed, "nothing committed");
    // read_uncommitted (logged above) depends on linger.ms timing, see ProcessingExceptionIT
    assertEquals(true, readiness.contains("\"status\":\"DOWN\""), "readiness DOWN");
  }
}
