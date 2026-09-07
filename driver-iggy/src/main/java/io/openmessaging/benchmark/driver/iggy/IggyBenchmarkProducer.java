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


import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.hash.XxHash32;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.SendMessagesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer that batches {@code sendAsync} calls into {@code sendMessages} requests.
 *
 * <p>The worker calls {@code sendAsync} once per message from one load thread. Messages are
 * buffered per target partition and flushed when a batch reaches the configured size or byte limit,
 * or when the linger timer fires. Messages without a key go to a "balanced" batch that the SDK
 * routes round-robin over the partitions; keyed messages are bucketed with the same {@code
 * xxh32(key) % partitions} mapping the SDK uses for {@code Partitioning.messagesKey}. Every message
 * future completes when the reply for its batch arrives.
 */
public class IggyBenchmarkProducer implements BenchmarkProducer {

    private static final Logger log = LoggerFactory.getLogger(IggyBenchmarkProducer.class);

    /** Bucket key of messages sent without a key. Partition ids are never negative. */
    private static final long BALANCED_BUCKET = -1L;

    private final AsyncIggyTcpClient client;
    private final StreamId streamId;
    private final TopicId topicId;
    private final long partitionsCount;
    private final int batchSize;
    private final long batchBytes;
    private final ScheduledFuture<?> lingerTask;

    private final Object lock = new Object();

    /** Open batches by bucket key. Guarded by {@link #lock}. */
    private final Map<Long, Batch> buckets = new HashMap<>();

    /** Guarded by {@link #lock}. */
    private boolean closed;

    private final Set<CompletableFuture<SendMessagesResponse>> inFlight =
            ConcurrentHashMap.newKeySet();

    IggyBenchmarkProducer(
            AsyncIggyTcpClient client,
            StreamId streamId,
            TopicId topicId,
            long partitionsCount,
            IggyConfig config,
            ScheduledExecutorService lingerExecutor) {
        this.client = client;
        this.streamId = streamId;
        this.topicId = topicId;
        this.partitionsCount = Math.max(1, partitionsCount);
        this.batchSize = Math.max(1, config.producerBatchSize);
        this.batchBytes = Math.max(1, config.producerBatchBytes);
        long lingerMs = Math.max(1, config.producerLingerMs);
        this.lingerTask =
                lingerExecutor.scheduleAtFixedRate(
                        this::flushAll, lingerMs, lingerMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Message message = Message.of(header(payload.length), payload, null);
        long bucketKey = key.map(this::partitionFor).orElse(BALANCED_BUCKET);

        Batch full = null;
        synchronized (lock) {
            if (closed) {
                future.completeExceptionally(new IllegalStateException("Producer is closed"));
                return future;
            }
            Batch batch = buckets.computeIfAbsent(bucketKey, Batch::new);
            batch.add(message, future);
            if (batch.messages.size() >= batchSize || batch.bytes >= batchBytes) {
                buckets.remove(bucketKey);
                full = batch;
            }
        }
        if (full != null) {
            send(full);
        }
        return future;
    }

    private long partitionFor(String key) {
        return XxHash32.hashUnsigned(key.getBytes(StandardCharsets.UTF_8)) % partitionsCount;
    }

    private static MessageHeader header(int payloadLength) {
        return new MessageHeader(
                BigInteger.ZERO,
                MessageId.serverGenerated(),
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.valueOf(nowMicros()),
                0L,
                (long) payloadLength,
                BigInteger.ZERO);
    }

    private static long nowMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
    }

    private void flushAll() {
        List<Batch> ready;
        synchronized (lock) {
            if (buckets.isEmpty()) {
                return;
            }
            ready = new ArrayList<>(buckets.values());
            buckets.clear();
        }
        for (Batch batch : ready) {
            send(batch);
        }
    }

    private void send(Batch batch) {
        Partitioning partitioning =
                batch.bucketKey == BALANCED_BUCKET
                        ? Partitioning.balanced()
                        : Partitioning.partitionId(batch.bucketKey);
        CompletableFuture<SendMessagesResponse> sent;
        try {
            sent = client.messages().sendMessages(streamId, topicId, partitioning, batch.messages);
        } catch (RuntimeException e) {
            batch.fail(e);
            return;
        }
        inFlight.add(sent);
        // Runs on the netty event loop: only complete the worker's futures here.
        sent.whenComplete(
                (response, error) -> {
                    inFlight.remove(sent);
                    if (error == null) {
                        batch.complete();
                    } else {
                        log.warn(
                                "Failed to send {} messages to {}/{}: {}",
                                batch.messages.size(),
                                streamId,
                                topicId,
                                IggyBenchmarkDriver.unwrap(error).toString());
                        batch.fail(error);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        lingerTask.cancel(false);
        flushAll();
        IggyBenchmarkDriver.awaitQuietly(
                CompletableFuture.allOf(inFlight.toArray(new CompletableFuture[0])),
                "waiting for in-flight batches of " + topicId);
        IggyBenchmarkDriver.awaitQuietly(client.close(), "closing producer connection of " + topicId);
    }

    private static final class Batch {
        private final long bucketKey;
        private final List<Message> messages = new ArrayList<>();
        private final List<CompletableFuture<Void>> futures = new ArrayList<>();
        private long bytes;

        Batch(long bucketKey) {
            this.bucketKey = bucketKey;
        }

        void add(Message message, CompletableFuture<Void> future) {
            messages.add(message);
            futures.add(future);
            bytes += message.payload().length;
        }

        void complete() {
            for (CompletableFuture<Void> future : futures) {
                future.complete(null);
            }
        }

        void fail(Throwable error) {
            for (CompletableFuture<Void> future : futures) {
                future.completeExceptionally(error);
            }
        }
    }
}
