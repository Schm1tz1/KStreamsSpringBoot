package com.github.schm1tz1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Topology test without Spring context or broker. */
class StreamsPipelineTest {
  static final Logger logger = LoggerFactory.getLogger(StreamsPipelineTest.class);

  private static final String INPUT_TOPIC = "input-topic";
  private static final String OUTPUT_TOPIC = "output-topic";

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  @BeforeEach
  void setUp() {
    StreamsBuilder builder = new StreamsBuilder();
    new StreamsPipeline().pipeline(builder, INPUT_TOPIC, OUTPUT_TOPIC);
    Topology topology = builder.build();
    logger.info("Topology under test:\n{}", topology.describe());

    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    testDriver = new TopologyTestDriver(topology, properties);

    inputTopic =
        testDriver.createInputTopic(
            INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
    outputTopic =
        testDriver.createOutputTopic(
            OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void passesRecordsThroughUnchanged() {
    pipe("key1", "value1");

    assertEquals("value1", read().value);
    assertTrue(outputTopic.isEmpty());
  }

  @Test
  void preservesKeyAndOrderingAcrossMultipleRecords() {
    pipe("key1", "value1");
    pipe("key2", "value2");

    var first = read();
    var second = read();

    assertEquals("key1", first.key);
    assertEquals("value1", first.value);
    assertEquals("key2", second.key);
    assertEquals("value2", second.value);
    assertTrue(outputTopic.isEmpty());
  }

  private void pipe(String key, String value) {
    logger.info("-> {} : {}={}", INPUT_TOPIC, key, value);
    inputTopic.pipeInput(key, value);
  }

  private KeyValue<String, String> read() {
    KeyValue<String, String> record = outputTopic.readKeyValue();
    logger.info("<- {} : {}={}", OUTPUT_TOPIC, record.key, record.value);
    return record;
  }
}
