package com.github.schm1tz1;

import static com.github.schm1tz1.MonitoringIT.BROKER;
import static com.github.schm1tz1.MonitoringIT.CLIENT;
import static com.github.schm1tz1.MonitoringIT.awaitTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.ObjectName;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * EOS v2 transaction timeout. A processor blocks inside an open transaction, depending on the
 * value:
 *
 * <ul>
 *   <li>"slow": longer than transaction.timeout.ms. The broker's transaction coordinator aborts the
 *       transaction and fences the producer, Streams recovers and reprocesses. read_committed
 *       consumers still see the record exactly once.
 *   <li>"fast": well below transaction.timeout.ms. The transaction commits normally, no fencing.
 * </ul>
 */
// close app + embedded broker after the class: both IT classes run in one JVM and share JMX
@DirtiesContext
@AutoConfigureObservability(tracing = false)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties =
        "spring.kafka.streams.properties.transaction.timeout.ms="
            + TransactionTimeoutIT.TRANSACTION_TIMEOUT_MS)
@EmbeddedKafka(
    topics = {
      "input-topic",
      "output-topic",
      TransactionTimeoutIT.SLOW_INPUT,
      TransactionTimeoutIT.SLOW_OUTPUT
    },
    bootstrapServersProperty = "spring.kafka.bootstrap-servers",
    brokerProperties = {
      "transaction.state.log.replication.factor=1",
      "transaction.state.log.min.isr=1",
      // broker checks for timed-out transactions every 10s by default
      "transaction.abort.timed.out.transaction.cleanup.interval.ms=500"
    })
class TransactionTimeoutIT {
  static final Logger logger = LoggerFactory.getLogger(TransactionTimeoutIT.class);

  static final String SLOW_INPUT = "slow-input";
  static final String SLOW_OUTPUT = "slow-output";
  static final int TRANSACTION_TIMEOUT_MS = 1000;
  static final Map<String, Integer> BLOCK_MS_BY_VALUE =
      Map.of("slow", 3 * TRANSACTION_TIMEOUT_MS, "fast", TRANSACTION_TIMEOUT_MS / 4);

  /** Block once per value only, the reprocessing after recovery must go through. */
  static final Set<String> blockedValues = ConcurrentHashMap.newKeySet();

  /** Extra sub-topology on the app's StreamsBuilder, so it shares the transactional producer. */
  @TestConfiguration
  static class SlowTopology {
    @Bean
    KStream<String, String> slowPipeline(StreamsBuilder builder) {
      KStream<String, String> stream =
          builder.stream(SLOW_INPUT, Consumed.with(Serdes.String(), Serdes.String()));
      stream
          .process(
              () ->
                  new ContextualProcessor<String, String, String, String>() {
                    @Override
                    public void process(Record<String, String> record) {
                      // forward first: the send opens the transaction the block runs in
                      context().forward(record);
                      Integer blockMs = BLOCK_MS_BY_VALUE.get(record.value());
                      if (blockMs != null && blockedValues.add(record.value())) {
                        logger.info(
                            "Blocking {}ms on '{}' inside open transaction (transaction.timeout.ms={})",
                            blockMs,
                            record.value(),
                            TRANSACTION_TIMEOUT_MS);
                        try {
                          Thread.sleep(blockMs);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        logger.info("Unblocked, Streams will now try to commit");
                      } else {
                        logger.info(
                            "Reprocessing {}={} without blocking", record.key(), record.value());
                      }
                    }
                  })
          .to(SLOW_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
      return stream;
    }
  }

  @Autowired TestRestTemplate rest;
  @Autowired KafkaTemplate<String, String> kafkaTemplate;

  @Value("${spring.kafka.bootstrap-servers}")
  String bootstrapServers;

  @Test
  void transactionTimeoutAbortsAndRecoversExactlyOnce() throws Exception {
    long brokerFencingBefore = brokerFencingErrors();
    logger.info("{} before send: fencing errors={}", BROKER, brokerFencingBefore);
    logger.info("Sending k=slow to {}", SLOW_INPUT);
    kafkaTemplate.send(SLOW_INPUT, "k", "slow").get();

    awaitTrue(
        "committed output after recovery",
        () -> !read("read_committed", "slow", Duration.ofSeconds(2)).isEmpty());

    List<String> committed = read("read_committed", "slow", Duration.ofSeconds(3));
    List<String> uncommitted = read("read_uncommitted", "slow", Duration.ofSeconds(3));
    logger.info("read_committed sees {}, read_uncommitted sees {}", committed, uncommitted);
    logMetrics("after recovery");
    logBrokerTxnMetrics();

    assertEquals(List.of("slow"), committed, "exactly once for read_committed");
    assertEquals(List.of("slow", "slow"), uncommitted, "aborted copy + committed copy");

    // The fence closes the thread's tasks dirty and recreates them: this is the alertable signal.
    assertTrue(
        metric("kafka_stream_thread_task_closed_total") > 0, CLIENT + " tasks closed on fencing");
    // The broker's coordinator aborted the transaction, not the client: the client-side abort
    // metric does not see transaction timeouts.
    assertEquals(
        0.0, metric("kafka_producer_txn_abort_time_ns_total"), CLIENT + " no client-side abort");
    // ...but the broker saw it: requests of the timed-out producer were rejected as fenced.
    assertTrue(brokerFencingErrors() > brokerFencingBefore, BROKER + " rejected fenced producer");
    // The time was spent processing, not committing: process-latency-max (per-record average of a
    // loop iteration; here one record per iteration) shows the block, while
    // commit-latency-max stays below transaction.timeout.ms (the commit did not cause the timeout).
    // Bound is the full timeout, not the 25% alert threshold: margin for slow CI machines.
    assertTrue(
        threadMetric("process-latency-max") >= BLOCK_MS_BY_VALUE.get("slow"),
        CLIENT + " process-latency-max shows the block");
    assertTrue(
        threadMetric("commit-latency-max") < TRANSACTION_TIMEOUT_MS,
        CLIENT + " commit-latency-max does not see the timeout");
  }

  @Test
  void transactionBelowTimeoutCommitsWithoutFencing() throws Exception {
    double tasksClosedBefore = metric("kafka_stream_thread_task_closed_total");
    double commitsBefore = metric("kafka_stream_thread_commit_total");
    long brokerFencingBefore = brokerFencingErrors();
    long brokerEndTxnBefore = brokerRequests("EndTxn");
    logger.info(
        "{} before send: task_closed_total={}, commit_total={}",
        CLIENT,
        tasksClosedBefore,
        commitsBefore);
    logger.info(
        "{} before send: fencing errors={}, EndTxn requests={}",
        BROKER,
        brokerFencingBefore,
        brokerEndTxnBefore);

    logger.info("Sending k=fast to {}", SLOW_INPUT);
    kafkaTemplate.send(SLOW_INPUT, "k", "fast").get();

    awaitTrue(
        "committed output", () -> !read("read_committed", "fast", Duration.ofSeconds(2)).isEmpty());

    List<String> committed = read("read_committed", "fast", Duration.ofSeconds(3));
    List<String> uncommitted = read("read_uncommitted", "fast", Duration.ofSeconds(3));
    logger.info("read_committed sees {}, read_uncommitted sees {}", committed, uncommitted);
    logMetrics("after commit");
    logBrokerTxnMetrics();

    assertEquals(List.of("fast"), committed, "exactly once for read_committed");
    assertEquals(List.of("fast"), uncommitted, "no aborted copy");
    assertEquals(
        tasksClosedBefore,
        metric("kafka_stream_thread_task_closed_total"),
        CLIENT + " no tasks closed");
    assertTrue(metric("kafka_stream_thread_commit_total") > commitsBefore, CLIENT + " committed");
    assertEquals(brokerFencingBefore, brokerFencingErrors(), BROKER + " no fencing");
    assertTrue(brokerRequests("EndTxn") > brokerEndTxnBefore, BROKER + " received EndTxn");
    assertTrue(
        threadMetric("commit-latency-max") < TRANSACTION_TIMEOUT_MS,
        CLIENT + " commit-latency-max below transaction.timeout.ms");
  }

  private void logMetrics(String when) {
    String scrape = rest.getForObject("/actuator/prometheus", String.class);
    logger.info(
        "{} transaction metrics {}:{}{}",
        CLIENT,
        when,
        // Streams producer only, ExampleProducer (producer-1) is not transactional
        MetricsReport.client(
            scrape,
            "kafka.producer:type=producer-metrics,client-id=*StreamThread-*",
            a -> a.startsWith("txn-")),
        MetricsReport.client(
            scrape,
            "kafka.streams:type=stream-thread-metrics,thread-id=*",
            a ->
                a.equals("commit-total")
                    || a.equals("task-closed-total")
                    || a.startsWith("commit-latency-")
                    || a.startsWith("process-latency-")));
  }

  /** Live stream-thread metric from JMX (no Micrometer refresh delay), max over all threads. */
  private static double threadMetric(String attribute) {
    return MetricsReport.query("kafka.streams:type=stream-thread-metrics,thread-id=*").stream()
        .mapToDouble(n -> ((Number) MetricsReport.attribute(n, attribute)).doubleValue())
        .max()
        .orElse(Double.NaN);
  }

  private double metric(String name) {
    return rest.getForObject("/actuator/prometheus", String.class)
        .lines()
        .filter(l -> l.startsWith(name + "{"))
        .mapToDouble(l -> Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1)))
        .sum();
  }

  private static final Set<String> TXN_REQUESTS =
      Set.of(
          "InitProducerId",
          "AddPartitionsToTxn",
          "AddOffsetsToTxn",
          "TxnOffsetCommit",
          "EndTxn",
          "WriteTxnMarkers");

  /** Transactional requests the broker rejected because the producer was fenced. */
  private static long brokerFencingErrors() {
    return MetricsReport.count(
            "kafka.network:type=RequestMetrics,name=ErrorsPerSec,error=PRODUCER_FENCED,*")
        + MetricsReport.count(
            "kafka.network:type=RequestMetrics,name=ErrorsPerSec,error=INVALID_PRODUCER_EPOCH,*");
  }

  private static long brokerRequests(String request) {
    return MetricsReport.count(
        "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=" + request + ",*");
  }

  private static void logBrokerTxnMetrics() {
    StringBuilder sb = new StringBuilder();
    for (ObjectName n :
        MetricsReport.query("kafka.network:type=RequestMetrics,name=RequestsPerSec,*")) {
      if (TXN_REQUESTS.contains(n.getKeyProperty("request"))) {
        sb.append(MetricsReport.broker("requests " + n.getKeyProperty("request"), n, "Count"));
      }
    }
    for (ObjectName n :
        MetricsReport.query("kafka.network:type=RequestMetrics,name=ErrorsPerSec,*")) {
      if (TXN_REQUESTS.contains(n.getKeyProperty("request"))
          && !"NONE".equals(n.getKeyProperty("error"))) {
        String label = "errors " + n.getKeyProperty("request") + "/" + n.getKeyProperty("error");
        sb.append(MetricsReport.broker(label, n, "Count"));
      }
    }
    for (String request : List.of("AddPartitionsToTxn", "AddOffsetsToTxn", "EndTxn")) {
      for (ObjectName n :
          MetricsReport.query(
              "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=" + request)) {
        sb.append(MetricsReport.broker("total time ms " + request + " max", n, "Max"));
        sb.append(MetricsReport.broker("total time ms " + request + " p99", n, "99thPercentile"));
      }
    }
    for (ObjectName n : MetricsReport.query("kafka.server:type=transaction-coordinator-metrics")) {
      sb.append(
          MetricsReport.broker("coordinator partition load ms max", n, "partition-load-time-max"));
      sb.append(
          MetricsReport.broker("coordinator partition load ms avg", n, "partition-load-time-avg"));
    }
    for (String name : List.of("PartitionsWithLateTransactionsCount", "ProducerIdCount")) {
      for (ObjectName n : MetricsReport.query("kafka.server:type=ReplicaManager,name=" + name)) {
        sb.append(MetricsReport.broker(name, n, "Value"));
      }
    }
    logger.info("{} transaction metrics:{}", BROKER, sb);
  }

  private List<String> read(String isolationLevel, String value, Duration pollFor) {
    return read(bootstrapServers, SLOW_OUTPUT, isolationLevel, value, pollFor);
  }

  /** Matching values on a topic, read from the beginning with a fresh consumer group. */
  static List<String> read(
      String bootstrapServers,
      String topic,
      String isolationLevel,
      String value,
      Duration pollFor) {
    Map<String, Object> props =
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG,
            "verify-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            isolationLevel,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class);
    List<String> values = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      Instant deadline = Instant.now().plus(pollFor);
      while (Instant.now().isBefore(deadline)) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
          if (value.equals(r.value())) {
            values.add(r.value());
          }
        }
      }
    }
    return values;
  }
}
