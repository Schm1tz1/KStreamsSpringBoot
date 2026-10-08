# Kafka Streams Spring Boot App

Spring Boot port of [KStreamsTemplate](https://github.com/Schm1tz1/KStreamsTemplate).
The processing logic is a pass-through stub (`ExampleStreamProcessor`, Processor API with
custom `processor-in`/`processor-out` sensors).

- `KStreamsSpringBootApp`: `@EnableKafkaStreams` (Spring manages the `KafkaStreams` lifecycle)
  and `@EnableRetry`.
- `StreamsPipeline`: builds the topology on the Spring-provided `StreamsBuilder`.
- `ExampleProducer`: sends to the input topic with `KafkaTemplate`, `MessageBuilder`/`KafkaHeaders`,
  and `@Retryable`/`@Backoff`.

## Build, Test, Run

```bash
mvn package                              # build + unit tests (fmt check runs too)
mvn verify                               # + MonitoringIT (embedded broker, ~60s)
mvn com.spotify.fmt:fmt-maven-plugin:format
mvn test -Dtest=StreamsPipelineTest      # single test
java -jar target/KStreamsSpringBoot-0.1.jar
java -jar target/KStreamsSpringBoot-0.1.jar --spring.config.additional-location=file:/path/to/app.yml
```

## Fat Jar and Container Image

- Fat jar: `mvn package` produces the executable `target/KStreamsSpringBoot-0.1.jar` (Spring Boot repackage).
- Image: Jib, no Docker daemon or Dockerfile needed. Base `gcr.io/distroless/java17-debian12:nonroot`
  (uid 65532), port 8080, `-XX:MaxRAMPercentage=75`.

```bash
mvn package jib:build -Dimage=registry/repo:tag   # multi-arch (amd64+arm64) push, uses ~/.docker/config.json creds
mvn package jib:buildTar -Djib.from.platforms=linux/arm64   # single-arch target/jib-image.tar, load with docker/podman load -i
podman run -p 8080:8080 -e SPRING_KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -v $PWD/my-app.yml:/app/config/application.yml schmitzi/kstreams-spring-boot:0.1
```

Platforms: linux/amd64 and linux/arm64. `jib:buildTar` fails with "multi-platform image building not supported when building a local tar image" unless `-Djib.from.platforms` names one platform. Distroless has no shell, so `kubectl exec` debugging needs `kubectl debug` with an ephemeral container.

`.mvn/settings.xml` overrides `~/.m2/settings.xml` so only Maven Central is used
(avoids internal mirrors).

## Monitoring

Replaces the template's JMX Prometheus javaagent (port 1234) with Actuator + Micrometer on port 8080:

- `/actuator/prometheus`: all Kafka Streams, consumer and producer metrics, including the custom
  `processor-in`/`processor-out` sensors (series names contain `processor_in_total` / `processor_out_total`).
  New metrics, e.g. per-task metrics after a rebalance, show up with up to 60s delay (Micrometer
  `KafkaMetrics` refresh interval).
- Exactly-once (`processing.guarantee: exactly_once_v2`) transaction metrics of the stream thread's
  transactional producer (client id `...-StreamThread-N-producer`). These are cumulative times,
  not counts: `kafka_producer_txn_{init,begin,send_offsets,commit,abort}_time_ns_total`. Use
  `kafka_stream_thread_commit_total`/`_rate`/`_latency_avg` for the commit count (one transaction per commit).
  A growing `txn_abort_time_ns_total` means the client aborted transactions. Transaction *timeouts*
  (processing longer than `transaction.timeout.ms`, which defaults to 10s under EOS) are aborted
  by the broker and never show up there. They show up as a `ProducerFencedException`/`TaskMigratedException`
  WARN log, a rebalance, and a rising `kafka_stream_thread_task_closed_total`. Right after the producer is
  recreated, the `txn_*` series can show stale values for up to 60s (Micrometer refresh). See `TransactionTimeoutIT`.
- Broker side (JMX on the brokers, not exposed by this app): the timeout shows up as
  `kafka.network:type=RequestMetrics,name=ErrorsPerSec,error=PRODUCER_FENCED` for transactional requests
  (e.g. `request=AddOffsetsToTxn`). Another sign is `WriteTxnMarkers` growing faster than the clients'
  `EndTxn` requests, because the coordinator writes abort markers on its own.
- `/actuator/health/liveness`, `/actuator/health/readiness`: Kubernetes probes. Readiness includes
  `KafkaStreamsHealthIndicator` (UP while `RUNNING`/`REBALANCING`).
- Kafka MBeans are still registered in JMX, so the javaagent or JMX remote from the template still
  works if needed.

## Configuration

`src/main/resources/application.yml`: `spring.kafka.*` for Kafka/Streams (any client property
goes under `spring.kafka.streams.properties` or `spring.kafka.properties`), and
`streamsApp.inputTopic` / `streamsApp.outputTopic` for the pipeline. Any value can be
overridden by env var, e.g. `SPRING_KAFKA_BOOTSTRAP_SERVERS=broker:9092`.
