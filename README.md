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
- Exactly-once transaction metrics, client and broker side: see [Transaction Metrics](#transaction-metrics-eos-v2).
- `/actuator/health/liveness`, `/actuator/health/readiness`: Kubernetes probes. Readiness includes
  `KafkaStreamsHealthIndicator` (UP while `RUNNING`/`REBALANCING`).
- Kafka MBeans are still registered in JMX, so the javaagent or JMX remote from the template still
  works if needed.

## Transaction Metrics (EOS v2)

With `processing.guarantee: exactly_once_v2`, each stream thread has one transactional producer
(client id `...-StreamThread-N-producer`). Records are committed in a transaction every
`commit.interval.ms` (100ms under EOS). The transaction must finish within `transaction.timeout.ms`
(Streams default 10s). If it doesn't, the broker's transaction coordinator aborts it and fences the
producer. Streams then gets `ProducerFencedException` -> `TaskMigratedException`, closes the
thread's tasks, rebalances, and reprocesses from the last committed offsets. Output stays exactly
once for `read_committed` consumers, but you pay for the timeout plus a rebalance, and the work
is reprocessed.

Apache Kafka 3.9 has **no counter or rate of committed, aborted or timed-out transactions**. The
producer only reports cumulative times (there is no `txn-abort-rate`), and the AK broker only reports
coordinator partition-load times. Count commits with the stream thread's `commit-total`, and detect
timeouts through fencing. Confluent Server adds broker-side transaction metrics, including a
timed-out transaction count; see the broker table.

### Client (this app, `/actuator/prometheus`)

Prometheus name = `kafka_` + MBean `type` without `-metrics` + `_` + attribute, with `-` replaced
by `_`. Cumulative counts that don't already end in `-total` get a `_total` suffix (Prometheus
counter naming). Producer metrics carry the `client_id` label, thread metrics the `thread_id` label.

| JMX MBean / attribute | Prometheus series | Meaning | Consequence for the transaction / what to do |
|---|---|---|---|
| `kafka.producer:type=producer-metrics,client-id=*-StreamThread-N-producer` / `txn-init-time-ns-total` | `kafka_producer_txn_init_time_ns_total` | Time spent in `initTransactions()` (startup, and again after each fence). | A new producer (after a fence or thread replacement) starts again from 0, so use `rate()`/`increase()`, which handle counter resets. A high value means the coordinator is slow or unreachable. |
| ... / `txn-begin-time-ns-total` | `kafka_producer_txn_begin_time_ns_total` | Time spent in `beginTransaction()` (local, near 0). | Only for completeness. |
| ... / `txn-send-offsets-time-ns-total` | `kafka_producer_txn_send_offsets_time_ns_total` | Time spent adding the consumed offsets to the transaction (`AddOffsetsToTxn` + `TxnOffsetCommit`). | Rising rate: the coordinator / group coordinator path is slow and uses up the transaction budget. |
| ... / `txn-commit-time-ns-total` | `kafka_producer_txn_commit_time_ns_total` | Time spent in `commitTransaction()`: flushing the batches plus the `EndTxn` round trip. The coordinator writes the markers asynchronously afterwards. | Divide its rate by the `commit-total` rate for the average commit time. Compare against `transaction.timeout.ms`. |
| ... / `txn-abort-time-ns-total` | `kafka_producer_txn_abort_time_ns_total` | Time spent in **client-initiated** `abortTransaction()`. | Rises when the client aborts, e.g. on an uncaught exception (`ProcessingExceptionIT`: about 3.4ms on the failed thread's producer). **Does not rise on transaction timeouts**: Streams does call `abortTransaction()` on the fenced producer, but that call throws `ProducerFencedException` (and Streams swallows it), and the producer records the time only after a successful abort. The same goes for `txn-commit-time-ns-total`: failed commits aren't recorded. |
| `kafka.streams:type=stream-thread-metrics,thread-id=*` / `commit-total`, `commit-rate` | `kafka_stream_thread_commit_total` / `_rate` | Commits of the thread. Under EOS, one commit = one transaction. | This is the transaction count. A commit rate of 0 while there is lag means no transaction completes. |
| ... / `commit-latency-avg`, `commit-latency-max` | `kafka_stream_thread_commit_latency_avg` / `_max` | Time to commit (flush + offsets + `EndTxn`), in ms. | Recorded per loop iteration as commit time / number of committed tasks, so it is the **average per task**, not the duration of the whole commit. Multiply by the task count for the full commit time. Alert above 25% of `transaction.timeout.ms` (threshold from the internal EOS runbook, calibrate it): the commit path eats the budget. It does **not** cover time spent processing. In the timeout test it stays at about 119ms while the transaction times out. |
| ... / `process-latency-avg`, `process-latency-max` | `kafka_stream_thread_process_latency_avg` / `_max` | Recorded per loop iteration as processing time / records processed, so it is the **average time per record** in that iteration (Kafka docs: "execution time in ms, for processing"). | Shows single slow records only when few records are processed per iteration. In `TransactionTimeoutIT` one record took 3006ms (timeout 1000ms). With up to `max.poll.records` (Streams default 1000) records per iteration, one slow record is averaged away, and many moderately slow records can exceed `transaction.timeout.ms` without any high value here. Use it together with `process-ratio` and the broker-side open-transaction age (Confluent Server). Fix with smaller `max.poll.records`, faster processors, or a larger `transaction.timeout.ms` (at most the broker's `transaction.max.timeout.ms`). |
| ... / `poll-latency-avg`, `poll-latency-max` | `kafka_stream_thread_poll_latency_*` | Time spent in `poll()`. | High values in an open transaction also use up the budget (the broker or network is slow). |
| ... / `task-closed-total`, `task-closed-rate` | `kafka_stream_thread_task_closed_total` / `_rate` | Tasks closed, including the dirty closes after a fence. | Alert on unexpected increases outside deployments: together with the `TaskMigratedException` WARN log, this is the client-side timeout/fencing signal. |
| `kafka.streams:type=stream-metrics,client-id=*` / `failed-stream-threads`, `alive-stream-threads` | `kafka_stream_failed_stream_threads_total` (counter, Prometheus adds `_total`), `kafka_stream_alive_stream_threads` | Stream threads that died from an uncaught exception, and threads still running. | With `REPLACE_THREAD`, each failure aborts the thread's open transaction and the work is reprocessed. A steadily rising `failed-stream-threads` means a poison pill is looping. `alive-stream-threads` = 0 means the client is down. Both are available immediately. The metrics of a replacement thread (`StreamThread-2`, ...) reach Prometheus only after the next Micrometer refresh (up to 60s). See `ProcessingExceptionIT`. |
| `kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*-StreamThread-N-consumer` / `records-lag-max` | `kafka_consumer_fetch_manager_records_lag_max` | Consumer lag. | Growing lag together with fencing means the app keeps reprocessing the same transaction. |
| `kafka.producer:type=producer-metrics,...` / `record-error-rate`, `record-retry-rate`, `request-latency-max` | `kafka_producer_record_error_rate` etc. | Send errors, retries, request latency. | Retries and slow requests inside a transaction push it towards the timeout. |

Caveat: after a fence the producer is recreated, and Micrometer keeps showing the old producer's
`txn_*` values until its next refresh (every 60s). The thread metrics are not affected by a fence,
because the thread stays. With `REPLACE_THREAD` the thread *is* replaced, so the new thread's
series also appear only after the refresh.

### Broker (JMX on the brokers; in tests, the embedded Apache Kafka broker)

The embedded test broker is Apache Kafka, so the Confluent Server rows are not covered by the tests.
Confluent Cloud exposes no broker JMX: use the Metrics API and the client-side metrics there.

| JMX MBean / attribute | Meaning | Consequence for the transaction / what to do |
|---|---|---|
| `kafka.network:type=RequestMetrics,name=ErrorsPerSec,request={AddPartitionsToTxn,AddOffsetsToTxn,TxnOffsetCommit,EndTxn},error=PRODUCER_FENCED` (or `INVALID_PRODUCER_EPOCH`) / `Count` | Transactional requests rejected because the producer epoch was bumped, e.g. by a timeout abort. | **The broker-side timeout/fencing signal**: alert on any increase. In `TransactionTimeoutIT` it shows as `AddOffsetsToTxn/PRODUCER_FENCED = 1`. |
| `kafka.network:type=RequestMetrics,name=TotalTimeMs,request={AddPartitionsToTxn,AddOffsetsToTxn,EndTxn}` / `Max`, `99thPercentile` | Broker-side latency of the transactional requests. | Alert when p99 exceeds 25% of `transaction.timeout.ms` (internal EOS runbook threshold). Client timeouts with low broker `TotalTimeMs` point to the network or a client pause, not the broker. |
| `kafka.network:type=RequestMetrics,name=RequestsPerSec,request={EndTxn,WriteTxnMarkers,InitProducerId}` / `Count` | Commit/abort requests from clients, transaction markers the coordinator writes, producer (re)initialisations. | On the single test broker every `EndTxn` led to one `WriteTxnMarkers` request, and the timeout added one extra (the coordinator's own abort). On a real cluster the coordinator sends markers to each partition leader involved, so the ratio depends on the topology. Only a change against the usual ratio hints at coordinator-side aborts. Rising `InitProducerId` means producers are being recreated. |
| `kafka.server:type=transaction-coordinator-metrics` / `partition-load-time-max`, `partition-load-time-avg` | Time to load `__transaction_state` partitions after a coordinator move. | While a partition loads, its transactions get `COORDINATOR_LOAD_IN_PROGRESS` and stall. High values follow broker restarts or leader changes. |
| `kafka.server:type=ReplicaManager,name=PartitionsWithLateTransactionsCount` / `Value` | Partitions with a transaction open longer than `transaction.max.timeout.ms` **+ 5 min** (`ProducerStateManager.LATE_TRANSACTION_BUFFER_MS`), i.e. 20 min by default. | Hanging transactions block `read_committed` consumers (the last stable offset can't advance). Does **not** count normal `transaction.timeout.ms` expiries. |
| **Confluent Server only**: `kafka.server:type=transaction-coordinator-metrics` / `transaction-timeout-count`, `transaction-timeout-rate` | Transactions the coordinator aborted because they exceeded their `transaction.timeout.ms`. | **The direct timeout counter** that Apache Kafka lacks: alert on any increase. It has no producer/application label, so correlate with the clients' fencing logs. |
| **Confluent Server only**: same MBean / `active-transaction-total-time-max` | Age of the oldest open transaction on this coordinator (max only, no count). | Rising towards `transaction.timeout.ms` means open transactions are close to timing out. Confluent Cloud's internal hanging-transaction alerts use this metric. |
| **Confluent Server only**: same MBean / `prepare-commit-to-complete-commit-time-max`, `completing-transaction-total-time-max`, `transaction-state-error-count`, `transaction-state-error-rate` | Marker phase duration (PREPARE_COMMIT to COMPLETE_COMMIT), time spent completing transactions, errors writing transaction state. | A slow marker phase delays when `read_committed` consumers see data. State errors mean `__transaction_state` writes fail. |
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`, `__transaction_state` partitions | Replication health, including the transaction log. | An unhealthy `__transaction_state` (below `transaction.state.log.min.isr`) makes transactional requests fail. |

## Configuration

`src/main/resources/application.yml`: `spring.kafka.*` for Kafka/Streams (any client property
goes under `spring.kafka.streams.properties` or `spring.kafka.properties`), and
`streamsApp.inputTopic` / `streamsApp.outputTopic` for the pipeline. Any value can be
overridden by env var, e.g. `SPRING_KAFKA_BOOTSTRAP_SERVERS=broker:9092`.

`streamsApp.uncaughtExceptionResponse` sets what happens when an exception escapes a processor.
It defaults to `REPLACE_THREAD`:

- `REPLACE_THREAD`: the thread's open transaction is aborted, a new thread takes over its tasks
  and reprocesses from the last commit. The client stays `RUNNING` and readiness stays UP. Under
  EOS the output is still exactly once (`ProcessingExceptionIT`). A record that fails every time
  makes this loop forever, so handle poison pills in the processor and alert on
  `failed-stream-threads`.
- `SHUTDOWN_CLIENT` (Kafka's behavior when no handler is set): the transaction is aborted and the
  client goes to `ERROR`, so it stops processing and readiness goes DOWN
  (`ProcessingExceptionShutdownIT`). Kubernetes stops routing to the pod, but the app needs a
  restart to process again. The liveness group does not include `kafkaStreams`, so Kubernetes
  does **not** restart the pod by itself.
- `SHUTDOWN_APPLICATION`: like `SHUTDOWN_CLIENT`, for all instances of the `application.id`.
