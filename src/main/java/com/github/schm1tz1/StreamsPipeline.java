package com.github.schm1tz1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Kafka Streams Pipeline, built on the StreamsBuilder provided by @EnableKafkaStreams */
@Configuration
public class StreamsPipeline {
  static final Logger logger = LoggerFactory.getLogger(StreamsPipeline.class);

  @Bean
  public KStream<String, String> pipeline(
      StreamsBuilder builder,
      @Value("${streamsApp.inputTopic}") String inputTopicName,
      @Value("${streamsApp.outputTopic}") String outputTopicName) {

    logger.info("Creating topology for " + inputTopicName + " -> " + outputTopicName);

    KStream<String, String> stream =
        builder.stream(inputTopicName, Consumed.with(Serdes.String(), Serdes.String()));
    stream
        .process(ExampleStreamProcessor::new)
        .to(outputTopicName, Produced.with(Serdes.String(), Serdes.String()));

    return stream;
  }
}
