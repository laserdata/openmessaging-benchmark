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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
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

public class IggyBenchmarkProducer implements BenchmarkProducer {
    private static final Logger log = LoggerFactory.getLogger(IggyBenchmarkProducer.class);
    private static final long BALANCED_BUCKET = -1L;
    private static final long UNASSIGNED = -1L;
    private static final int DRAIN_BATCH_LIMIT = 64;
    private static final long CLOSE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    /** Pause between two replays of an operation that stayed uncertain. */
    private static final long RESOLVE_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    private final AsyncIggyTcpClient client;
    private final StreamId streamId;
    private final TopicId topicId;
    private final long partitionsCount;
    private final int batchSize;
    private final long batchBytes;
    private final long lingerNanos;
    private final long maxPendingBytes;
    private final long closeTimeoutNanos;
    private final ScheduledExecutorService lingerExecutor;
    private final Executor flushExecutor;
    private final LongSupplier clock;
    private final Semaphore inFlightPermits;
    private final UncertainOperations.Policy uncertainPolicy;
    private final Duration resolveTimeout;
    private final long uncertainTimeoutNanos;
    private final Object lock = new Object();
    private final Map<Long, Batch> buckets = new HashMap<>();
    private final Deque<Batch> ready = new ArrayDeque<>();
    private final Set<Batch> pending = new HashSet<>();
    /** Blocked lanes by the id of the operation that blocks them. */
    private final Map<Long, Recovery> recoveries = new HashMap<>();
    /** Blocked lanes by partition, so that a batch for a blocked partition waits unsent. */
    private final Map<Long, Recovery> blockedPartitions = new HashMap<>();
    private boolean draining;
    private boolean closed;
    private boolean closeStarted;
    private boolean terminated;
    private Throwable terminalError;
    private long pendingBytes;
    private long balancedPartition;
    private long nextSequence;
    private ScheduledFuture<?> lingerDeadline;

    IggyBenchmarkProducer(
            AsyncIggyTcpClient client,
            StreamId streamId,
            TopicId topicId,
            long partitionsCount,
            IggyConfig config,
            ScheduledExecutorService lingerExecutor,
            Executor flushExecutor) {
        this(
                client,
                streamId,
                topicId,
                partitionsCount,
                config,
                new Execution(lingerExecutor, flushExecutor, System::nanoTime, CLOSE_TIMEOUT_NANOS));
    }

    IggyBenchmarkProducer(
            AsyncIggyTcpClient client,
            StreamId streamId,
            TopicId topicId,
            long partitionsCount,
            IggyConfig config,
            Execution execution) {
        this.client = client;
        this.streamId = streamId;
        this.topicId = topicId;
        this.partitionsCount = Math.max(1, partitionsCount);
        this.batchSize = Math.max(1, config.producerBatchSize);
        this.batchBytes = Math.max(1, config.producerBatchBytes);
        this.lingerNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1, config.producerLingerMs));
        this.lingerExecutor = execution.scheduler();
        this.flushExecutor = execution.executor();
        this.clock = execution.clock();
        this.closeTimeoutNanos = execution.closeTimeoutNanos();
        this.maxPendingBytes = config.resolvedMaxPendingBytes();
        this.inFlightPermits =
                config.producerMaxInFlightBatches > 0
                        ? new Semaphore(config.producerMaxInFlightBatches)
                        : null;
        this.uncertainPolicy = UncertainOperations.Policy.parse(config.uncertainOperationPolicy);
        this.resolveTimeout = Duration.ofMillis(Math.max(1, config.requestTimeoutMs));
        this.uncertainTimeoutNanos =
                TimeUnit.MILLISECONDS.toNanos(Math.max(0, config.uncertainOperationTimeoutMs));
    }

    record Execution(
            ScheduledExecutorService scheduler,
            Executor executor,
            LongSupplier clock,
            long closeTimeoutNanos) {}

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Message message = Message.of(header(payload.length), payload, null);
        long bucketKey = key.map(this::partitionFor).orElse(BALANCED_BUCKET);
        long chargedBytes = (long) payload.length + MessageHeader.SIZE;
        Throwable schedulingFailure = null;
        synchronized (lock) {
            try {
                while (!closed
                        && maxPendingBytes > 0
                        && pendingBytes > 0
                        && chargedBytes > maxPendingBytes - pendingBytes) {
                    lock.wait();
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(error);
                return future;
            }
            if (closed) {
                future.completeExceptionally(
                        terminalError == null
                                ? new IllegalStateException("Producer is closed")
                                : new IllegalStateException(
                                        "Producer is closed after a fatal error", terminalError));
                return future;
            }
            Batch batch = buckets.get(bucketKey);
            if (batch == null) {
                batch = new Batch(bucketKey, clock.getAsLong());
                buckets.put(bucketKey, batch);
                pending.add(batch);
            }
            batch.add(message, future, chargedBytes);
            pendingBytes += chargedBytes;
            if (batch.messages.size() >= batchSize || batch.bytes >= batchBytes) {
                buckets.remove(bucketKey);
                markReady(batch);
            } else {
                try {
                    armLingerDeadline(batch);
                } catch (RuntimeException error) {
                    schedulingFailure = error;
                }
            }
        }
        if (schedulingFailure != null) {
            terminate(schedulingFailure);
        } else {
            scheduleDrain();
        }
        return future;
    }

    private void armLingerDeadline(Batch batch) {
        if (lingerDeadline == null && !closed && !batch.ready) {
            long delay = Math.max(0, lingerNanos - (clock.getAsLong() - batch.createdNanos));
            lingerDeadline = lingerExecutor.schedule(this::onLingerDeadline, delay, TimeUnit.NANOSECONDS);
        }
    }

    // Call under lock. A ready batch stays open until it fills or the sender takes it.
    private void markReady(Batch batch) {
        if (!batch.ready) {
            batch.ready = true;
            ready.addLast(batch);
        }
    }

    private void onLingerDeadline() {
        try {
            synchronized (lock) {
                lingerDeadline = null;
                if (closed) {
                    return;
                }
                long now = clock.getAsLong();
                Batch oldest = null;
                for (Batch batch : buckets.values()) {
                    if (batch.ready) {
                        continue;
                    }
                    long age = now - batch.createdNanos;
                    if (age >= lingerNanos) {
                        markReady(batch);
                    } else if (oldest == null || age > now - oldest.createdNanos) {
                        oldest = batch;
                    }
                }
                if (oldest != null) {
                    armLingerDeadline(oldest);
                }
            }
            scheduleDrain();
        } catch (Throwable error) {
            terminate(error);
        }
    }

    private void scheduleDrain() {
        synchronized (lock) {
            if (terminated
                    || draining
                    || ready.isEmpty()
                    || (inFlightPermits != null && inFlightPermits.availablePermits() == 0)) {
                return;
            }
            draining = true;
        }
        try {
            flushExecutor.execute(this::drain);
        } catch (Throwable error) {
            synchronized (lock) {
                draining = false;
            }
            terminate(error);
        }
    }

    private void drain() {
        try {
            for (int count = 0; count < DRAIN_BATCH_LIMIT; count++) {
                Batch batch;
                synchronized (lock) {
                    if (terminated || ready.isEmpty()) {
                        return;
                    }
                    if (inFlightPermits != null && !inFlightPermits.tryAcquire()) {
                        return;
                    }
                    batch = ready.removeFirst();
                    buckets.remove(batch.bucketKey, batch);
                    batch.permitHeld = inFlightPermits != null;
                }
                send(batch);
            }
        } catch (Throwable error) {
            terminate(error);
        } finally {
            synchronized (lock) {
                draining = false;
                lock.notifyAll();
            }
            scheduleDrain();
        }
    }

    private void send(Batch batch) {
        try {
            long partition;
            synchronized (lock) {
                if (batch.finished || terminated) {
                    return;
                }
                if (batch.partition == UNASSIGNED) {
                    // Fixed at the first send, so a batch that waited for a blocked lane goes out
                    // to the same partition and keeps its place in the send order.
                    if (batch.bucketKey == BALANCED_BUCKET) {
                        batch.partition = balancedPartition;
                        balancedPartition = (balancedPartition + 1) % partitionsCount;
                    } else {
                        batch.partition = batch.bucketKey;
                    }
                    batch.sequence = nextSequence++;
                }
                partition = batch.partition;
                Recovery recovery = blockedPartitions.get(partition);
                if (recovery != null) {
                    park(recovery, batch);
                    return;
                }
            }
            CompletableFuture<SendMessagesResponse> sent =
                    client
                            .messages()
                            .sendMessages(streamId, topicId, Partitioning.partitionId(partition), batch.messages);
            sent.whenComplete((response, error) -> onReply(batch, error));
        } catch (Throwable error) {
            onReply(batch, error);
        }
    }

    /**
     * Finishes the batch, or holds it while the operation that blocks its lane is settled. An
     * uncertain batch went out, so it waits for the outcome of its own operation. A blocked batch
     * never went out, so it goes out again once the lane opens.
     *
     * @param batch the batch that was sent
     * @param error the failure of the send, or null when it succeeded
     */
    private void onReply(Batch batch, Throwable error) {
        OptionalLong blocker =
                error == null ? OptionalLong.empty() : UncertainOperations.blockingOperation(error);
        if (blocker.isEmpty()) {
            finish(batch, error);
            return;
        }
        long operationId = blocker.getAsLong();
        Recovery started = null;
        boolean held;
        synchronized (lock) {
            if (batch.finished) {
                return;
            }
            held = !terminated;
            if (held) {
                Recovery recovery = recoveries.get(operationId);
                if (recovery == null) {
                    recovery = new Recovery(operationId, batch.partition, clock.getAsLong());
                    recoveries.put(operationId, recovery);
                    started = recovery;
                }
                blockedPartitions.putIfAbsent(batch.partition, recovery);
                if (UncertainOperations.isUncertain(error)) {
                    // Its request is the operation, so it stays in flight until the outcome is known.
                    recovery.uncertain = batch;
                } else {
                    park(recovery, batch);
                }
            }
        }
        if (!held) {
            finish(batch, error);
            return;
        }
        if (started != null) {
            log.warn(
                    "Operation {} on {}/{} partition {} has an unknown outcome and blocks the partition;"
                            + " {} it",
                    Long.toUnsignedString(operationId),
                    streamId,
                    topicId,
                    started.partition,
                    uncertainPolicy == UncertainOperations.Policy.RESOLVE ? "resolving" : "abandoning");
            settle(started);
        }
        scheduleDrain();
    }

    // Call under lock. A parked batch is off the wire, so it gives back its in-flight permit. Its
    // bytes stay charged until it is sent again and answered.
    private void park(Recovery recovery, Batch batch) {
        if (batch.permitHeld) {
            batch.permitHeld = false;
            inFlightPermits.release();
        }
        recovery.parked.add(batch);
    }

    private void settle(Recovery recovery) {
        long deadline = recovery.startedNanos + uncertainTimeoutNanos;
        UncertainOperations.settle(
                        client,
                        recovery.operationId,
                        uncertainPolicy,
                        resolveTimeout,
                        command -> lingerExecutor.schedule(command, RESOLVE_BACKOFF_NANOS, TimeUnit.NANOSECONDS),
                        () -> stopResolving(deadline))
                .whenComplete((ignored, error) -> endRecovery(recovery, error));
    }

    private boolean stopResolving(long deadline) {
        synchronized (lock) {
            if (terminated) {
                return true;
            }
        }
        return uncertainTimeoutNanos > 0 && clock.getAsLong() - deadline >= 0;
    }

    /**
     * Ends the wait for a blocked lane. The uncertain batch gets the outcome of its operation, and the
     * parked batches go back to the front of the send queue in their original order.
     *
     * @param recovery the blocked lane
     * @param error null when a replay proved that the operation committed, else the outcome
     */
    private void endRecovery(Recovery recovery, Throwable error) {
        Batch uncertain;
        int resent = 0;
        synchronized (lock) {
            recoveries.remove(recovery.operationId, recovery);
            if (blockedPartitions.remove(recovery.partition, recovery)) {
                for (Recovery other : recoveries.values()) {
                    if (other.partition == recovery.partition) {
                        blockedPartitions.put(other.partition, other);
                        break;
                    }
                }
            }
            uncertain = recovery.uncertain;
            recovery.parked.sort(Comparator.comparingLong(batch -> batch.sequence));
            for (int index = recovery.parked.size() - 1; index >= 0; index--) {
                Batch batch = recovery.parked.get(index);
                if (!batch.finished && !terminated) {
                    ready.addFirst(batch);
                    resent++;
                }
            }
            recovery.parked.clear();
        }
        log.warn(
                "Operation {} on {}/{} partition {} {} after {} ms; {} waiting batches go out again",
                Long.toUnsignedString(recovery.operationId),
                streamId,
                topicId,
                recovery.partition,
                error == null ? "committed" : "ended with " + IggyBenchmarkDriver.unwrap(error),
                TimeUnit.NANOSECONDS.toMillis(clock.getAsLong() - recovery.startedNanos),
                resent);
        if (uncertain != null) {
            finish(uncertain, error);
        }
        scheduleDrain();
    }

    private void finish(Batch batch, Throwable error) {
        synchronized (lock) {
            if (batch.finished) {
                return;
            }
            batch.finished = true;
            pending.remove(batch);
            pendingBytes -= batch.chargedBytes;
            if (batch.permitHeld) {
                batch.permitHeld = false;
                inFlightPermits.release();
            }
            lock.notifyAll();
        }
        for (CompletableFuture<Void> future : batch.futures) {
            if (error == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(error);
            }
        }
        scheduleDrain();
    }

    private void terminate(Throwable error) {
        List<Batch> abandoned;
        boolean reportCause;
        synchronized (lock) {
            if (terminated) {
                return;
            }
            closed = true;
            terminated = true;
            if (lingerDeadline != null) {
                lingerDeadline.cancel(false);
                lingerDeadline = null;
            }
            abandoned = new ArrayList<>(pending);
            reportCause = !closeStarted || !abandoned.isEmpty();
            if (reportCause) {
                terminalError = error;
            }
            ready.clear();
            buckets.clear();
            lock.notifyAll();
        }
        if (reportCause) {
            Throwable cause = IggyBenchmarkDriver.unwrap(error);
            log.warn("Producer for {}/{} terminated: {}", streamId, topicId, cause.toString(), cause);
        }
        for (Batch batch : abandoned) {
            finish(batch, error);
        }
    }

    @Override
    public void close() throws Exception {
        long deadline = System.nanoTime() + closeTimeoutNanos;
        synchronized (lock) {
            if (closeStarted) {
                return;
            }
            closeStarted = true;
            closed = true;
            if (lingerDeadline != null) {
                lingerDeadline.cancel(false);
                lingerDeadline = null;
            }
            buckets.values().forEach(this::markReady);
            buckets.clear();
            lock.notifyAll();
        }
        scheduleDrain();
        try {
            synchronized (lock) {
                long remaining;
                while (!pending.isEmpty() && (remaining = deadline - System.nanoTime()) > 0) {
                    TimeUnit.NANOSECONDS.timedWait(lock, remaining);
                }
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        } finally {
            terminate(new IllegalStateException("Producer closed before acknowledgement"));
            CompletableFuture<Void> closing = client.close();
            try {
                closing.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (Exception error) {
                if (error instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.warn("Closing producer {}: {}", topicId, IggyBenchmarkDriver.unwrap(error).toString());
            }
        }
    }

    private long partitionFor(String key) {
        return XxHash32.hashUnsigned(key.getBytes(StandardCharsets.UTF_8)) % partitionsCount;
    }

    private static MessageHeader header(int payloadLength) {
        Instant now = Instant.now();
        long micros = now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
        return new MessageHeader(
                BigInteger.ZERO,
                MessageId.serverGenerated(),
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.valueOf(micros),
                0L,
                (long) payloadLength,
                BigInteger.ZERO);
    }

    private static final class Batch {
        private final long bucketKey;
        private final long createdNanos;
        private final List<Message> messages = new ArrayList<>();
        private final List<CompletableFuture<Void>> futures = new ArrayList<>();
        private long bytes;
        private long chargedBytes;
        private boolean ready;
        private boolean permitHeld;
        private boolean finished;
        /** The partition of every send of this batch, set at the first one. */
        private long partition = UNASSIGNED;
        /** The place of the first send in the producer's send order. */
        private long sequence;

        Batch(long bucketKey, long createdNanos) {
            this.bucketKey = bucketKey;
            this.createdNanos = createdNanos;
        }

        void add(Message message, CompletableFuture<Void> future, long charge) {
            messages.add(message);
            futures.add(future);
            bytes += message.payload().length;
            chargedBytes += charge;
        }
    }

    /** A lane that one operation blocks, and the batches that wait for it to open. */
    private static final class Recovery {
        private final long operationId;
        private final long partition;
        private final long startedNanos;
        private final List<Batch> parked = new ArrayList<>();
        /** The batch whose request is the operation, once its uncertain reply came back. */
        private Batch uncertain;

        Recovery(long operationId, long partition, long startedNanos) {
            this.operationId = operationId;
            this.partition = partition;
            this.startedNanos = startedNanos;
        }
    }
}
