package com.github.schm1tz1;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Test-only sub-topology on the app's StreamsBuilder (shares the transactional producer): forwards
 * each record, then throws once per value starting with "boom", so the exception escapes the stream
 * thread with an open transaction. Import it with {@code @Import(FailingTopology.class)}.
 */
@TestConfiguration
class FailingTopology {
  static final Logger logger = LoggerFactory.getLogger(FailingTopology.class);

  static final String FAIL_INPUT = "fail-input";
  static final String FAIL_OUTPUT = "fail-output";

  /** Throw once per value only, the reprocessing after a thread replacement must go through. */
  static final Set<String> thrownValues = ConcurrentHashMap.newKeySet();

  @Bean
  KStream<String, String> failingPipeline(StreamsBuilder builder) {
    KStream<String, String> stream =
        builder.stream(FAIL_INPUT, Consumed.with(Serdes.String(), Serdes.String()));
    stream
        .process(
            () ->
                new ContextualProcessor<String, String, String, String>() {
                  @Override
                  public void process(Record<String, String> record) {
                    // forward first: the send opens the transaction the exception leaves behind
                    context().forward(record);
                    if (record.value().startsWith("boom") && thrownValues.add(record.value())) {
                      logger.info("Throwing on '{}' inside open transaction", record.value());
                      throw new IllegalStateException(
                          "simulated processing failure on " + record.value());
                    }
                    logger.info("Processed '{}' without failing", record.value());
                  }
                })
        .to(FAIL_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
    return stream;
  }
}
