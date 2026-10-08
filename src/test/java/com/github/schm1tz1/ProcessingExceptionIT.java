package com.github.schm1tz1;

import static com.github.schm1tz1.FailingTopology.FAIL_INPUT;
import static com.github.schm1tz1.FailingTopology.FAIL_OUTPUT;
import static com.github.schm1tz1.MonitoringIT.CLIENT;
import static com.github.schm1tz1.MonitoringIT.awaitTrue;
import static com.github.schm1tz1.TransactionTimeoutIT.read;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Exception in a processor with the app's default uncaught exception response REPLACE_THREAD: the
 * failing thread aborts its transaction and is replaced (StreamThread-2), the record is reprocessed
 * exactly once.
 */
@DirtiesContext
@AutoConfigureObservability(tracing = false)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(FailingTopology.class)
@EmbeddedKafka(
    topics = {"input-topic", "output-topic", FAIL_INPUT, FAIL_OUTPUT},
    bootstrapServersProperty = "spring.kafka.bootstrap-servers",
    brokerProperties = {
      "transaction.state.log.replication.factor=1",
      "transaction.state.log.min.isr=1"
    })
class ProcessingExceptionIT {
  static final Logger logger = LoggerFactory.getLogger(ProcessingExceptionIT.class);

  @Autowired TestRestTemplate rest;
  @Autowired KafkaTemplate<String, String> kafkaTemplate;

  @Value("${spring.kafka.bootstrap-servers}")
  String bootstrapServers;

  @Test
  void replaceThreadAbortsAndReprocessesExactlyOnce() throws Exception {
    logger.info("Sending k=boom-replace to {}", FAIL_INPUT);
    kafkaTemplate.send(FAIL_INPUT, "k", "boom-replace").get();

    awaitTrue(
        "committed output after thread replacement",
        () ->
            !read(
                    bootstrapServers,
                    FAIL_OUTPUT,
                    "read_committed",
                    "boom-replace",
                    Duration.ofSeconds(2))
                .isEmpty());

    List<String> committed =
        read(
            bootstrapServers, FAIL_OUTPUT, "read_committed", "boom-replace", Duration.ofSeconds(3));
    List<String> uncommitted =
        read(
            bootstrapServers,
            FAIL_OUTPUT,
            "read_uncommitted",
            "boom-replace",
            Duration.ofSeconds(3));
    logger.info("read_committed sees {}, read_uncommitted sees {}", committed, uncommitted);
    String scrape = rest.getForObject("/actuator/prometheus", String.class);
    logger.info(
        "{} after thread replacement:{}{}",
        CLIENT,
        MetricsReport.client(
            scrape,
            "kafka.streams:type=stream-metrics,client-id=*",
            a -> a.equals("alive-stream-threads") || a.equals("failed-stream-threads")),
        MetricsReport.client(
            scrape,
            "kafka.producer:type=producer-metrics,client-id=*StreamThread-*",
            a -> a.startsWith("txn-")));
    String readiness = rest.getForObject("/actuator/health/readiness", String.class);
    logger.info("readiness: {}", readiness);

    assertEquals(List.of("boom-replace"), committed, "exactly once for read_committed");
    // read_uncommitted usually sees no aborted copy: the exception follows the forward() before the
    // producer batch is sent ("TransactionAbortedException: Failing batch"). That depends on
    // linger.ms timing, so it is logged above, not asserted.
    assertEquals(1.0, clientMetric("failed-stream-threads"), CLIENT + " one thread failed");
    assertEquals(1.0, clientMetric("alive-stream-threads"), CLIENT + " replacement is alive");
    assertTrue(readiness.contains("\"status\":\"UP\""), "readiness UP after replacement");
  }

  /** Live client-level Kafka Streams metric from JMX. */
  static double clientMetric(String attribute) {
    return MetricsReport.query("kafka.streams:type=stream-metrics,client-id=*").stream()
        .mapToDouble(n -> ((Number) MetricsReport.attribute(n, attribute)).doubleValue())
        .sum();
  }
}
