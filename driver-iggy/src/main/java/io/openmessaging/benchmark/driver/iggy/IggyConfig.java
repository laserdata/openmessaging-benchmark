/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.iggy;


import java.util.List;
import java.util.Map;

/**
 * Driver settings read from the driver yaml (see {@code iggy.yaml}).
 *
 * <p>Unknown keys fail the run at start-up, so a misspelled key cannot silently fall back to its
 * default. The two framework keys every driver yaml carries are declared here for that reason.
 */
public class IggyConfig {

    /** Framework key present in every driver yaml; not read by the driver. */
    public String name;

    /** Framework key present in every driver yaml; not read by the driver. */
    public String driverClass;

    /** Host of the Iggy TCP listener. */
    public String host = "127.0.0.1";

    /** Port of the Iggy TCP listener. */
    public int port = 8090;

    /**
     * Bootstrap addresses as {@code host:port} ({@code host} alone uses {@link #port}). Connections
     * pick the next entry round-robin and fall through to the following one when a dial or login
     * fails. Empty means {@link #host} and {@link #port}. The SDK retargets every login to the
     * metadata leader, so the list buys start-up resilience only, not load spreading.
     */
    public List<String> hosts = List.of();

    /** User every connection logs in with. */
    public String username = "iggy";

    /** Password every connection logs in with. */
    public String password = "iggy";

    /**
     * Netty event loop threads shared by every connection this driver instance opens (admin,
     * producers, consumers). One loop per core is plenty: the loops only do socket I/O and frame
     * decoding, while producing runs on the worker's load threads and every consumer has its own poll
     * thread. Default is the smaller of 8 and the core count. 0 disables sharing and gives every
     * connection its own single-thread loop, the SDK default.
     */
    public int ioThreads = Math.min(8, Runtime.getRuntime().availableProcessors());

    /** Dial timeout of every connection, in milliseconds. */
    public long connectionTimeoutMs = 3000;

    /**
     * Time a request may wait for its reply before the SDK fails it, in milliseconds. An overloaded
     * partition drops requests without a reply, so this bounds how late such a failure surfaces.
     */
    public long requestTimeoutMs = 30000;

    /**
     * Redial policy after a lost connection: {@code default} (the SDK default, 12 attempts 5 s
     * apart), {@code none}, {@code exponential} (3 attempts, 100 ms doubling up to 5 s) or {@code
     * fixed} ({@link #retryMaxRetries} attempts {@link #retryDelayMs} apart). Requests that were in
     * flight when the connection dropped fail either way; the policy only restores the connection.
     */
    public String retryPolicy = "default";

    /** Redial attempts for {@code retryPolicy: fixed}. */
    public int retryMaxRetries = 12;

    /** Delay between redial attempts for {@code retryPolicy: fixed}, in milliseconds. */
    public long retryDelayMs = 5000;

    /** Stream that holds the benchmark topics. Created when missing. */
    public String streamName = "omb";

    /**
     * Topic options passed at CreateTopic, keyed by server option name (for example {@code
     * enforce_fsync}, {@code messages_required_to_save}, {@code segment_size}). Values are sent as
     * strings and parsed by the server the way the {@code iggy topic create --set} CLI values are.
     * Empty means server defaults.
     */
    public Map<String, String> topicOptions = Map.of();

    /** A producer batch is flushed once it holds this many messages. */
    public int producerBatchSize = 1000;

    /** A producer batch is flushed once its payloads reach this many bytes. */
    public long producerBatchBytes = 1024 * 1024;

    /**
     * How long an open producer batch waits for more messages before it is sent, whatever its size.
     * The deadline runs from the batch's first message, so a bucket taking a steady trickle cannot
     * postpone its own flush.
     */
    public long producerLingerMs = 1;

    /** Batches a producer may have in flight before its send lane stops. 0 disables the cap. */
    public int producerMaxInFlightBatches = 16;

    /**
     * Bytes a producer may hold unacknowledged, across open, queued and in-flight batches, before
     * {@code sendAsync} blocks the worker's load thread. That block is the driver's backpressure.
     *
     * <p>A batch count alone does not bound memory. At 100 topics of 3 MB/s each batch closes on the
     * linger deadline holding a few kilobytes, so the in-flight cap is reached with a small fraction
     * of the memory the same cap permits when batches are full. 0 disables the budget and leaves the
     * batch count as the only bound. Negative derives it as {@link #producerMaxInFlightBatches} whole
     * batches, which is what the batch cap already implies once batches fill.
     */
    public long producerMaxPendingBytes = -1;

    /**
     * Threads shared by every producer in this worker for encoding and submitting batches.
     *
     * <p>The linger timer only moves a batch onto its producer's send queue, so this pool is where
     * the per-batch work actually runs. Sizing it below the core count is deliberate: the Netty loops
     * and the worker's load threads need the rest.
     */
    public int producerFlushThreads = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Maximum number of messages one consumer poll asks for. */
    public int consumerPollSize = 1000;

    /**
     * Polls one consumer may have in flight, across distinct partitions and never more than one per
     * partition. 1 reproduces the original one-at-a-time sweep.
     *
     * <p>A consumer owning P partitions and polling them one at a time revisits each once per P round
     * trips, so a message arriving just after its partition was visited waits most of a sweep.
     * Raising this shortens the revisit interval without changing what a single poll asks for, which
     * is the client half of the partition-count latency growth.
     */
    public int consumerPollConcurrency = 1;

    // true: the server stores the group offset after every poll (one replicated operation
    // per non-empty poll). false: the consumer keeps its own cursor per owned partition and
    // stores the offset itself, at most once per consumerCommitIntervalMs per partition
    // (0 = after every non-empty poll, the Kafka driver's commitAsync shape).
    public boolean consumerAutoCommit = true;
    public long consumerCommitIntervalMs = 0;

    /**
     * How long the server may hold a poll that has nothing to return, in milliseconds. 0 polls
     * immediately and returns whatever is resident, which costs one request per empty result and one
     * idle backoff before the next attempt.
     *
     * <p>Above 0 the consumer issues a deferred poll instead: the server holds it until {@link
     * #consumerDeferredMinCount} messages are readable, the byte cap prevents selecting more, or this
     * wait expires. It needs a server that answers command 105; an older one fails the poll with
     * {@code InvalidCommand} and the driver does not fall back.
     */
    public long consumerDeferredMaxWaitMs = 0;

    /** Messages that make a deferred poll ready. Never more than {@link #consumerPollSize}. */
    public long consumerDeferredMinCount = 1;

    /**
     * Cap on the encoded body of a deferred poll response, including framing but not the transport
     * header. A smaller reply than {@link #consumerPollSize} messages is normal; the cursor advances
     * by what arrived. The server's own read cap (16 MiB by default) still applies.
     */
    public long consumerDeferredMaxBytes = 4L * 1024 * 1024;

    /**
     * {@link #producerMaxPendingBytes} with the negative "derive it" case resolved.
     *
     * @return the byte budget one producer may hold unacknowledged, or 0 when unbounded
     */
    long resolvedMaxPendingBytes() {
        if (producerMaxPendingBytes >= 0) {
            return producerMaxPendingBytes;
        }
        if (producerMaxInFlightBatches <= 0) {
            return 0;
        }
        try {
            return Math.multiplyExact(Math.max(1, producerBatchBytes), producerMaxInFlightBatches);
        } catch (ArithmeticException error) {
            throw new IllegalArgumentException(
                    "Derived producer byte budget exceeds signed 64-bit range", error);
        }
    }
}
