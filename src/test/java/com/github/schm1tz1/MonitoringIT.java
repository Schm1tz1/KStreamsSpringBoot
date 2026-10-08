package com.github.schm1tz1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * End-to-end: embedded broker, record through the pipeline, custom sensor and EOS transaction
 * metrics visible to Prometheus.
 */
// @SpringBootTest disables metrics export (and so /actuator/prometheus) unless this is present
// fresh app + embedded broker per test: broker metrics start at 0, and all IT classes share one
// JVM (and its JMX)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@AutoConfigureObservability(tracing = false)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
    topics = {"input-topic", "output-topic"},
    bootstrapServersProperty = "spring.kafka.bootstrap-servers",
    // single broker: transactions (EOS v2) need the txn state log to fit on one node
    brokerProperties = {
      "transaction.state.log.replication.factor=1",
      "transaction.state.log.min.isr=1"
    })
class MonitoringIT {
  static final Logger logger = LoggerFactory.getLogger(MonitoringIT.class);

  /** Log prefixes so client-side (this app) and broker-side metrics are easy to tell apart. */
  static final String CLIENT = "[CLIENT app, /actuator/prometheus]";

  static final String BROKER = "[BROKER embedded, JMX]";

  @Autowired TestRestTemplate rest;
  @Autowired ExampleProducer producer;

  @Test
  void exposesHealthAndCustomStreamsMetrics() {
    awaitTrue(
        "readiness UP",
        () -> {
          String readiness = rest.getForObject("/actuator/health/readiness", String.class);
          logger.trace("readiness: {}", readiness);
          return readiness.contains("UP");
        });

    logger.info("Sending key1=value1 to input-topic");
    producer.send("key1", "value1");

    // Micrometer KafkaMetrics picks up new Kafka metrics only every 60s, so sensors created on
    // task assignment appear with up to that delay.
    awaitTrue(
        "custom sensor processor_in_total > 0 (up to 60s Micrometer refresh)",
        () -> {
          String series = series("processor_in_total");
          logger.trace("processor_in_total series: {}", series.isEmpty() ? "<none yet>" : series);
          return series.lines().anyMatch(l -> !l.endsWith(" 0.0"));
        });
    logger.info(
        "{} custom sensor metrics:{}",
        CLIENT,
        MetricsReport.client(
            rest.getForObject("/actuator/prometheus", String.class),
            "kafka.streams:type=stream-kstreams-template-metrics,*",
            a -> a.startsWith("processor-")));
  }

  @Test
  void exposesEosTransactionMetrics() {
    // initTransactions() runs once the stream thread's transactional producer is up
    awaitTrue(
        "transactional producer initialized (txn_init_time_ns_total > 0)",
        () -> streamsProducerMetric("kafka_producer_txn_init_time_ns_total") > 0);
    double commitsBefore = metric("kafka_stream_thread_commit_total", "");
    double txnCommitTimeBefore = streamsProducerMetric("kafka_producer_txn_commit_time_ns_total");
    logger.info(
        "{} before send: stream_thread_commit_total={}, txn_commit_time_ns_total={}",
        CLIENT,
        commitsBefore,
        txnCommitTimeBefore);

    logger.info("Sending key2=value2 to input-topic");
    producer.send("key2", "value2");

    // EOS commits every 100ms by default (commit.interval.ms), each commit ends a transaction
    awaitTrue(
        "stream thread commit count increases",
        () -> metric("kafka_stream_thread_commit_total", "") > commitsBefore);
    awaitTrue(
        "txn_commit_time_ns_total increases",
        () ->
            streamsProducerMetric("kafka_producer_txn_commit_time_ns_total") > txnCommitTimeBefore);
    logger.info(
        "{} Streams producer transaction metrics:{}",
        CLIENT,
        MetricsReport.client(
            rest.getForObject("/actuator/prometheus", String.class),
            "kafka.producer:type=producer-metrics,client-id=*StreamThread-*",
            a -> a.startsWith("txn-")));
    assertTrue(
        streamsProducerMetric("kafka_producer_txn_begin_time_ns_total") > 0, CLIENT + " txn begin");
    assertTrue(
        streamsProducerMetric("kafka_producer_txn_send_offsets_time_ns_total") > 0,
        CLIENT + " txn send offsets");
    assertEquals(
        0.0,
        streamsProducerMetric("kafka_producer_txn_abort_time_ns_total"),
        CLIENT + " no txn abort");
  }

  /** Transaction metrics of the Streams producer only (client id "...-StreamThread-N-producer"). */
  private double streamsProducerMetric(String name) {
    return metric(name, "StreamThread-");
  }

  /** Sum of all series of a Prometheus metric whose labels contain labelFragment. */
  private double metric(String name, String labelFragment) {
    return scrape()
        .filter(l -> l.startsWith(name + "{") && l.contains(labelFragment))
        .mapToDouble(l -> Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1)))
        .sum();
  }

  /** Streams-client series whose name contains namePart, labels shortened for readability. */
  private String series(String namePart) {
    return scrape()
        .filter(l -> !l.startsWith("#") && l.contains("StreamThread-"))
        .filter(l -> l.substring(0, l.indexOf('{')).contains(namePart))
        .map(l -> "  " + l.replaceAll("\\{.*(StreamThread-[^\"]*)\".*}", "{$1}"))
        .collect(Collectors.joining("\n"));
  }

  private java.util.stream.Stream<String> scrape() {
    return rest.getForObject("/actuator/prometheus", String.class).lines();
  }

  static void awaitTrue(String description, Supplier<Boolean> condition) {
    logger.info("Waiting for: {}", description);
    Instant start = Instant.now();
    Instant deadline = start.plus(Duration.ofSeconds(90));
    while (Instant.now().isBefore(deadline)) {
      if (condition.get()) {
        logger.info(
            "OK after {}ms: {}", Duration.between(start, Instant.now()).toMillis(), description);
        return;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    assertTrue(condition.get(), "not met within 90s: " + description);
  }
}
