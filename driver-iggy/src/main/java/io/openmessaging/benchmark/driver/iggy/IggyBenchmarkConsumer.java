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


import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One consumer group member polling one topic on its own thread.
 *
 * <p>Two offset modes. With {@code consumerAutoCommit: true} every poll uses the server-side cursor
 * ({@link PollingStrategy#next()}) and asks the server to store the group offset after the poll. On
 * a replicated topic that store is one consensus operation per non-empty poll, on top of the
 * produce batch it acknowledges. With {@code consumerAutoCommit: false} the consumer keeps its own
 * cursor per owned partition, polls by explicit offset, and stores the offset itself: after every
 * non-empty poll when {@code consumerCommitIntervalMs} is 0 (the shape of the Kafka driver's {@code
 * commitAsync}), or at most once per interval per partition otherwise, plus once on close. The
 * owned partitions come from the sync-consumer-group command, which the server answers for the
 * calling connection. They are re-read every few seconds, after any poll failure and after a fenced
 * poll (the re-sync sentinel), so a rebalance moves the cursors with the assignment.
 */
public class IggyBenchmarkConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(IggyBenchmarkConsumer.class);

    private static final long IDLE_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long ERROR_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long ASSIGNMENT_REFRESH_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final long CLOSE_TIMEOUT_SECONDS = 30;
    private static final long COMMIT_FAILURES_LOGGED_VERBATIM = 10;
    // A fenced group poll (the member does not own the partition at the current generation) is
    // answered with an empty poll whose partition id is this sentinel; mirrors
    // RESYNC_REQUIRED_PARTITION_SENTINEL in the server and the SDK.
    private static final long RESYNC_REQUIRED_PARTITION_SENTINEL = 0xFFFF_FFFFL;

    private final AsyncIggyTcpClient client;
    private final StreamId streamId;
    private final TopicId topicId;
    private final ConsumerId groupId;
    private final Consumer consumer;
    private final long partitionsCount;
    private final long pollSize;
    private final boolean autoCommit;
    private final long commitIntervalNanos;
    private final ConsumerCallback callback;
    private final Thread pollThread;
    private final AtomicBoolean closed = new AtomicBoolean();

    // Client-cursor mode. Owned by the poll thread; close() reads it after join().
    private List<Long> owned = List.of();
    private final Map<Long, Cursor> cursors = new HashMap<>();
    private boolean assignmentStale = true;
    private long assignmentRefreshDeadlineNanos;
    private final Set<CompletableFuture<Void>> inFlightCommits = ConcurrentHashMap.newKeySet();
    private final AtomicLong commitFailures = new AtomicLong();

    IggyBenchmarkConsumer(
            AsyncIggyTcpClient client,
            StreamId streamId,
            TopicId topicId,
            ConsumerId groupId,
            long partitionsCount,
            IggyConfig config,
            ConsumerCallback callback) {
        this.client = client;
        this.streamId = streamId;
        this.topicId = topicId;
        this.groupId = groupId;
        this.consumer = Consumer.group(groupId);
        this.partitionsCount = Math.max(1, partitionsCount);
        this.pollSize = Math.max(1, config.consumerPollSize);
        this.autoCommit = config.consumerAutoCommit;
        this.commitIntervalNanos =
                TimeUnit.MILLISECONDS.toNanos(Math.max(0, config.consumerCommitIntervalMs));
        this.callback = callback;
        this.pollThread = new Thread(this::pollLoop, "iggy-consumer-" + topicId + "-" + groupId);
        this.pollThread.setDaemon(true);
        this.pollThread.start();
    }

    private void pollLoop() {
        if (autoCommit) {
            pollWithServerCursor();
        } else {
            pollWithClientCursor();
        }
    }

    // The server picks the partition among the ones this member owns, reads from the
    // stored group offset and stores the new one after the poll.
    private void pollWithServerCursor() {
        long emptyPolls = 0;
        while (!closed.get()) {
            try {
                PolledMessages polled =
                        client
                                .messages()
                                .pollMessages(
                                        streamId,
                                        topicId,
                                        Optional.empty(),
                                        consumer,
                                        PollingStrategy.next(),
                                        pollSize,
                                        true)
                                .get();
                if (polled.messages().isEmpty()) {
                    emptyPolls++;
                    if (emptyPolls >= partitionsCount) {
                        emptyPolls = 0;
                        LockSupport.parkNanos(IDLE_BACKOFF_NANOS);
                    }
                    continue;
                }
                emptyPolls = 0;
                deliver(polled);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (closed.get()) {
                    return;
                }
                logPollFailure(e);
                LockSupport.parkNanos(ERROR_BACKOFF_NANOS);
            }
        }
    }

    // Round-robin over the owned partitions, each polled by explicit offset from a local
    // cursor. Nothing is stored on the server until maybeCommit decides to.
    private void pollWithClientCursor() {
        long emptyPolls = 0;
        int next = 0;
        while (!closed.get()) {
            try {
                long now = System.nanoTime();
                if (assignmentStale || now - assignmentRefreshDeadlineNanos >= 0) {
                    refreshAssignment(now);
                }
                if (owned.isEmpty()) {
                    LockSupport.parkNanos(ERROR_BACKOFF_NANOS);
                    continue;
                }
                if (next >= owned.size()) {
                    next = 0;
                }
                long partition = owned.get(next++);
                Cursor cursor = cursors.get(partition);
                PolledMessages polled =
                        client
                                .messages()
                                .pollMessages(
                                        streamId,
                                        topicId,
                                        Optional.of(partition),
                                        consumer,
                                        PollingStrategy.offset(BigInteger.valueOf(cursor.next)),
                                        pollSize,
                                        false)
                                .get();
                if (polled.messages().isEmpty()) {
                    if (polled.partitionId() != null
                            && polled.partitionId() == RESYNC_REQUIRED_PARTITION_SENTINEL) {
                        // The server fenced the poll: the assignment moved. Re-sync before the
                        // next poll instead of spinning on the stale partition.
                        assignmentStale = true;
                        LockSupport.parkNanos(ERROR_BACKOFF_NANOS);
                        continue;
                    }
                    emptyPolls++;
                    if (emptyPolls >= owned.size()) {
                        emptyPolls = 0;
                        LockSupport.parkNanos(IDLE_BACKOFF_NANOS);
                    }
                    continue;
                }
                emptyPolls = 0;
                long lastOffset = deliver(polled);
                cursor.next = lastOffset + 1;
                cursor.dirty = true;
                maybeCommit(partition, cursor, false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (closed.get()) {
                    return;
                }
                logPollFailure(e);
                // Ownership may have moved (rebalance); re-read it before the next poll.
                assignmentStale = true;
                LockSupport.parkNanos(ERROR_BACKOFF_NANOS);
            }
        }
    }

    // Hands every message to the benchmark and returns the offset of the last one.
    private long deliver(PolledMessages polled) {
        long lastOffset = -1;
        for (Message message : polled.messages()) {
            // originTimestamp is the producer's wall clock in microseconds.
            long publishTimestampMs = message.header().originTimestamp().longValue() / 1_000L;
            callback.messageReceived(message.payload(), publishTimestampMs);
            lastOffset = message.header().offset().longValue();
        }
        return lastOffset;
    }

    // Asks the server for this member's assignment. The server identifies the member by the
    // calling connection, so no member id is matched here: the ids in the group details are
    // member slots, not client ids. The reply lists only the partitions this member may poll; a
    // partition pending hand-off to another member is left out until the hand-off completes. A
    // cursor for a newly owned partition starts after the group's stored offset, or at 0 when
    // none is stored (auto.offset.reset=earliest in Kafka terms).
    private void refreshAssignment(long now) throws Exception {
        Optional<ConsumerGroupAssignment> assignment =
                client.consumerGroups().syncConsumerGroup(streamId, topicId, groupId).get();
        List<Long> mine = assignment.map(a -> List.copyOf(a.partitions())).orElse(List.of());
        for (Iterator<Map.Entry<Long, Cursor>> it = cursors.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, Cursor> entry = it.next();
            if (!mine.contains(entry.getKey())) {
                maybeCommit(entry.getKey(), entry.getValue(), true);
                it.remove();
            }
        }
        for (long partition : mine) {
            if (!cursors.containsKey(partition)) {
                Cursor cursor = new Cursor();
                cursor.next = storedOffset(partition).map(offset -> offset + 1).orElse(0L);
                // The first commit of a fresh cursor is not held back by the interval.
                cursor.lastCommitNanos = now - commitIntervalNanos;
                cursors.put(partition, cursor);
            }
        }
        if (!mine.equals(owned)) {
            log.info(
                    "Consumer group {} on {}/{}: this member owns partitions {} (generation {})",
                    groupId,
                    streamId,
                    topicId,
                    mine,
                    assignment.map(ConsumerGroupAssignment::generation).orElse(-1L));
        }
        owned = mine;
        assignmentStale = mine.isEmpty();
        assignmentRefreshDeadlineNanos = now + ASSIGNMENT_REFRESH_NANOS;
    }

    private Optional<Long> storedOffset(long partition) throws Exception {
        try {
            Optional<ConsumerOffsetInfo> info =
                    client
                            .consumerOffsets()
                            .getConsumerOffset(streamId, topicId, Optional.of(partition), consumer)
                            .get();
            return info.map(ConsumerOffsetInfo::storedOffset).map(BigInteger::longValue);
        } catch (ExecutionException e) {
            if (IggyBenchmarkDriver.isNotFound(e)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    // Stores cursor.next - 1 unless the cursor is clean or the interval has not elapsed.
    // Fire-and-forget like Kafka's commitAsync; close() waits for the stragglers.
    private void maybeCommit(long partition, Cursor cursor, boolean force) {
        if (!cursor.dirty) {
            return;
        }
        long now = System.nanoTime();
        if (!force && commitIntervalNanos > 0 && now - cursor.lastCommitNanos < commitIntervalNanos) {
            return;
        }
        cursor.dirty = false;
        cursor.lastCommitNanos = now;
        BigInteger offset = BigInteger.valueOf(cursor.next - 1);
        CompletableFuture<Void> stored;
        try {
            stored =
                    client
                            .consumerOffsets()
                            .storeConsumerOffset(streamId, topicId, Optional.of(partition), consumer, offset);
        } catch (RuntimeException e) {
            logCommitFailure(partition, offset, e);
            return;
        }
        inFlightCommits.add(stored);
        stored.whenComplete(
                (ignored, error) -> {
                    inFlightCommits.remove(stored);
                    if (error != null) {
                        logCommitFailure(partition, offset, error);
                    }
                });
    }

    private void logPollFailure(Exception e) {
        log.warn(
                "Poll failed for {}/{} group {}: {}",
                streamId,
                topicId,
                groupId,
                IggyBenchmarkDriver.unwrap(e).toString());
    }

    private void logCommitFailure(long partition, BigInteger offset, Throwable error) {
        long failures = commitFailures.incrementAndGet();
        if (failures <= COMMIT_FAILURES_LOGGED_VERBATIM || failures % 1000 == 0) {
            log.warn(
                    "Storing offset {} of {}/{} partition {} for group {} failed ({} failures so far): {}",
                    offset,
                    streamId,
                    topicId,
                    partition,
                    groupId,
                    failures,
                    IggyBenchmarkDriver.unwrap(error).toString());
        }
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        pollThread.interrupt();
        try {
            pollThread.join(TimeUnit.SECONDS.toMillis(CLOSE_TIMEOUT_SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!autoCommit) {
            for (Map.Entry<Long, Cursor> entry : new ArrayList<>(cursors.entrySet())) {
                maybeCommit(entry.getKey(), entry.getValue(), true);
            }
            IggyBenchmarkDriver.awaitQuietly(
                    CompletableFuture.allOf(inFlightCommits.toArray(new CompletableFuture[0])),
                    "storing the final offsets of " + groupId);
        }
        IggyBenchmarkDriver.awaitQuietly(
                client.consumerGroups().leaveConsumerGroup(streamId, topicId, groupId),
                "leaving consumer group " + groupId);
        IggyBenchmarkDriver.awaitQuietly(client.close(), "closing consumer connection of " + groupId);
    }

    /** Client-side position of one owned partition. Poll thread only. */
    private static final class Cursor {
        long next;
        long lastCommitNanos;
        boolean dirty;
    }
}
