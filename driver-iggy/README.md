# Apache Iggy driver

Runs the OpenMessaging Benchmark against [Apache Iggy](https://iggy.apache.org/) over the TCP
transport, through the Java SDK (`org.apache.iggy:iggy`).

## Requirements

* JDK 17 and Maven 3.8+
* `iggy-server` 0.9.0 or a build from `master`. The driver needs the VSR wire protocol that 0.9.0
  introduces; 0.8.0 cannot log in. Until 0.9.0 is on Maven Central the module depends on
  `0.9.0-SNAPSHOT`, resolved from the ASF snapshot repository declared in its `pom.xml`.

## Running against a local server

1. Start the server on a fresh data directory with the default root credentials (`iggy` / `iggy`).
   Without `--with-default-root-credentials` a fresh data directory gets a random root password.

   ```
   iggy-server --fresh --with-default-root-credentials
   ```
2. Build the distribution from the repository root. `benchmark-framework` fails its own spotless and
   spotbugs checks at the current upstream master, so both are skipped for the whole build;
   `driver-iggy` passes them on its own (`mvn -pl driver-iggy verify`).

   ```
   mvn -DskipTests -Dspotless.check.skip=true -Dspotbugs.skip=true install
   ```
3. Unpack the tarball and run the one-minute smoke workload. With no `--workers` and no
   `workers.yaml` in the working directory the benchmark runs one in-process worker.

   ```
   tar xzf package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz
   cd openmessaging-benchmark-0.0.1-SNAPSHOT
   bin/benchmark --drivers driver-iggy/iggy.yaml driver-iggy/smoke-workload.yaml
   ```
4. Run the standard workloads and render the charts. `bin/benchmark` exits 0 even when a run fails,
   so check that the `publishLatency*` and `endToEndLatency*` series in the result JSON are non-zero.

   ```
   bin/benchmark --drivers driver-iggy/iggy.yaml,driver-iggy/iggy-fsync.yaml \
       workloads/1-topic-1-partition-1kb.yaml \
       workloads/max-rate-1-topic-16-partitions-1kb.yaml
   bin/create_charts.py *.json
   ```

## Configuration (`iggy.yaml`, `iggy-fsync.yaml`)

|                Key                |       Default       |                                                                       Meaning                                                                       |
|-----------------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `host`, `port`                    | `127.0.0.1`, `8090` | TCP listener of the server                                                                                                                          |
| `hosts`                           | `[]`                | Bootstrap list of `host:port`, tried round-robin with fall-through; overrides `host`/`port`                                                         |
| `username`, `password`            | `iggy`, `iggy`      | Credentials every connection logs in with                                                                                                           |
| `ioThreads`                       | `min(8, cores)`     | Netty event loop threads shared by every connection of the worker; `0` = one loop per connection                                                    |
| `connectionTimeoutMs`             | `3000`              | Dial timeout of every connection                                                                                                                    |
| `requestTimeoutMs`                | `30000`             | Time a request may wait for its reply; bounds how late a dropped request fails                                                                      |
| `retryPolicy`                     | `default`           | Redial after a lost connection: `default` (12 attempts 5 s apart), `none`, `exponential`, `fixed`                                                   |
| `retryMaxRetries`, `retryDelayMs` | `12`, `5000`        | Attempts and delay for `retryPolicy: fixed`                                                                                                         |
| `streamName`                      | `omb`               | Stream holding the benchmark topics, created when missing                                                                                           |
| `topicOptions`                    | `{}`                | Options passed at CreateTopic, keyed by server option name, values as strings                                                                       |
| `producerBatchSize`               | `1000`              | Flush a batch once it holds this many messages                                                                                                      |
| `producerBatchBytes`              | `1048576`           | Flush a batch once its payloads reach this many bytes                                                                                               |
| `producerLingerMs`                | `1`                 | Flush open batches this often, whatever their size                                                                                                  |
| `producerMaxInFlightBatches`      | `16`                | Batches a producer may have in flight before `sendAsync` blocks; `0` = no cap                                                                       |
| `consumerPollSize`                | `1000`              | Maximum number of messages one poll asks for                                                                                                        |
| `consumerAutoCommit`              | `true`              | `true`: the server stores the group offset after every poll. `false`: the consumer keeps a cursor per owned partition and stores the offsets itself |
| `consumerCommitIntervalMs`        | `0`                 | With `consumerAutoCommit: false`, the shortest time between two offset stores of one partition. `0` stores after every non-empty poll               |
| `consumerDeferredMaxWaitMs`       | `0`                 | Milliseconds the server may hold a poll that has nothing to return. `0` polls immediately; above `0` needs a server that answers command 105        |
| `consumerDeferredMinCount`        | `1`                 | With a readiness wait, the messages that make one poll ready                                                                                        |
| `consumerDeferredMaxBytes`        | `4194304`           | With a readiness wait, the cap on the encoded response body of one poll, in bytes                                                                   |

Unknown keys fail the run at start-up (the framework's `name` and `driverClass` are tolerated), so a
misspelled key cannot silently run with the default.

## How the driver maps the benchmark onto Iggy

* One admin connection creates the stream and the topics. Topics are created with
  `CompressionAlgorithm.None`, no message expiry and no size limit; everything else comes from
  `topicOptions`, or from the server defaults when it is empty (`enforce_fsync = false`,
  `messages_required_to_save = 1024`, `segment_size = 1 GiB`). The requested and the effective
  options are logged next to "Created topic". The topics a run created are deleted when the driver
  closes, so repeated runs do not fill the disk.
* fsync cells. `iggy-fsync.yaml` sets `enforce_fsync: "true"` and `messages_required_to_save: "1"`,
  which makes every publish acknowledgement wait for the batch to be fsynced. Acknowledgements are
  durability-gated only when `messages_required_to_save` is 1; a larger value fsyncs in the
  background. Values go over the wire as strings and are parsed by the server the way the
  `iggy topic create --set key=value` CLI values are, so `segment_size: "128MiB"` works too. A key
  or value the server refuses fails the run at topic creation.
* Every producer owns one connection. `sendAsync` stamps `originTimestamp` with the current wall
  clock in microseconds, buffers the message and flushes by size, bytes or linger as one
  `sendMessages` call. Messages without a key go with `Partitioning.balanced()`, which the SDK
  resolves to one partition per batch, rotating; keyed messages are bucketed by
  `xxh32(key) % partitions`, the mapping `Partitioning.messagesKey` uses, and each bucket is sent
  with `Partitioning.partitionId`. A message future completes when its batch is acknowledged, which
  means committed to the replicated log and visible to consumers (fsynced as well with the fsync
  options).
* Backpressure. At most `producerMaxInFlightBatches` batches are in flight per producer. A full
  batch blocks the worker's load thread until a permit is free, the same model as Kafka's
  `buffer.memory` and Pulsar's `blockIfQueueFull`, so `producerRate: 0` (max-rate discovery) and an
  overloaded server cannot grow the heap without bound. The shared linger thread never blocks: a
  batch it cannot send stays open until the next tick or until it fills. With the default
  1000-message, 1 MiB batches the cap is 16 MiB per producer.
* Every consumer owns one connection and one poll thread. The consumers of one subscription form one
  Iggy consumer group named after the subscription, so every subscription receives every message.
  With `consumerAutoCommit: true` polls use `PollingStrategy.next()` with auto-commit. The server
  picks one partition of the member's assignment per poll and stores the group offset after it.
  On a replicated topic that store is one consensus operation per non-empty poll. With `false`
  the consumer reads its assignment with the sync-consumer-group command, which the server answers
  for the calling connection. It re-reads it every 5 s, after a poll failure and after a fenced
  poll (an empty poll with the re-sync sentinel as partition id, sent while a rebalance moves the
  partition). It keeps a cursor per owned partition and polls each by explicit offset. It stores the
  offset itself, at most once per `consumerCommitIntervalMs` per partition and once on close. A
  fresh cursor starts after the group's stored offset, or at 0. In both modes the loop backs off
  only after a full empty cycle. End-to-end latency is `now - originTimestamp / 1000` in
  milliseconds.

## Notes

* The SDK and its runtime dependencies (netty 4.2, Jackson 3, httpclient5, commons-lang3, hash4j)
  are shaded and relocated into the driver jar, because the distribution ships only `netty-all`
  4.1 and strips every individual netty module from `lib/`. The shade plugin relocates netty's
  system property names as well; `bin/benchmark-worker` therefore sets
  `-Dio.openmessaging.benchmark.driver.iggy.shaded.io.netty.tryReflectionSetAccessible=true` next
  to the plain and the Pulsar-shaded variants.
* All connections of one worker share one netty event loop group of `ioThreads` loops (SDK
  `eventLoopGroup(...)`, needs `0.9.0-SNAPSHOT` build 20 or newer). The loops only do socket I/O
  and frame decoding: producing runs on the worker's load threads and every consumer has its own
  poll thread, so the thread count no longer grows with producers plus consumers. Completion
  callbacks run on the shared loops, which is why the producer never blocks inside one; the
  in-flight permit is taken on the load thread before a batch is sent. `ioThreads: 0` falls back to
  the SDK default of one single-thread loop per connection.
* In a cluster every login converges on the metadata leader: the SDK fetches the roster after login
  and retargets the connection, and at view 0 every partition's primary is replica 0. In a healthy
  run all client traffic therefore lands on one node and the followers only replicate. `hosts` buys
  start-up resilience when a node is down, not load spreading.
* Overloaded partitions drop requests without a reply once their prepare queue is full, so the
  client only learns about them after `requestTimeoutMs`. For runs of a few minutes a shorter value
  (10 s) surfaces such failures while the run is still going; `retryPolicy` only governs redialing
  after a lost connection, requests in flight at that moment fail either way.
* Every poll with auto-commit writes the consumer offset, and every offset write is a replicated
  partition-plane request. A "commit every N polls" knob is not offered: the server resolves
  `PollingStrategy.next()` from the stored offset, so polls without auto-commit re-serve the same
  messages, and a client-side offset strategy would have to pin partitions and lose the group
  assignment.
* Joining a group is cooperative with a 30 s rebalancing timeout. With `consumerPerSubscription > 1`
  late joiners get partitions only after the owner drains them or the timeout passes, so expect a
  startup transient inside the 60 s readiness window.

