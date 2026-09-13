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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    private static final long IDLE_BACKOFF_MILLIS = 1;
    private static final long IDLE_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(IDLE_BACKOFF_MILLIS);
    private static final long ERROR_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    /** How long an assignment refresh or a close waits for outstanding polls to report back. */
    private static final long DRAIN_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);

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
    private final int pollConcurrency;
    private final boolean autoCommit;
    private final long commitIntervalNanos;
    private final ConsumerCallback callback;
    private final Thread pollThread;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final Object completionLock = new Object();

    // Client-cursor mode. Owned by the poll thread; close() reads it after join().
    private List<Long> owned = List.of();
    private final Map<Long, Cursor> cursors = new HashMap<>();
    private boolean assignmentStale = true;
    private long assignmentRefreshDeadlineNanos;

    /**
     * Bumped whenever the owned set changes. Every issued poll carries the value it was issued under,
     * so a reply that crosses a rebalance is recognised and dropped instead of delivering messages
     * for a partition another member now owns.
     */
    private long assignmentGeneration;

    private Long coordinatorGeneration;
    private long nextPollId;

    /** Partitions with a poll on the wire. At most one entry per partition. */
    private final Map<Long, Long> inFlightPolls = new HashMap<>();

    /** Index into {@link #owned} the next sweep starts at, so every partition gets a turn. */
    private int sweepStart;

    /** Replies waiting for the poll thread to apply them. */
    private final BlockingQueue<Completion> completions;

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
        this.pollConcurrency = Math.max(1, config.consumerPollConcurrency);
        this.completions = new ArrayBlockingQueue<>(pollConcurrency);
        this.autoCommit = config.consumerAutoCommit;
        this.commitIntervalNanos =
                TimeUnit.MILLISECONDS.toNanos(Math.max(0, config.consumerCommitIntervalMs));
        this.callback = callback;
        this.pollThread = new Thread(this::pollLoop, "iggy-consumer-" + topicId + "-" + groupId);
        this.pollThread.setDaemon(true);
    }

    void start() {
        pollThread.start();
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

    /**
     * Polls the owned partitions by explicit offset from a local cursor, up to {@code
     * consumerPollConcurrency} at a time and never more than one per partition. Nothing is stored on
     * the server until {@link #maybeCommit} decides to.
     *
     * <p>This thread stays the only one that touches assignment, cursors and commits. Network
     * completions do nothing but hand a result back through {@link #completions}, so concurrency
     * changes how many requests are on the wire and nothing about who owns the state.
     *
     * <p>Every issued poll carries the assignment generation it was issued under. A rebalance bumps
     * the generation, so a reply for a partition this member no longer owns is dropped before it can
     * deliver messages or move a cursor that now belongs to someone else.
     */
    private void pollWithClientCursor() {
        while (!closed.get()) {
            try {
                long now = System.nanoTime();
                if (assignmentStale || now - assignmentRefreshDeadlineNanos >= 0) {
                    // Wait for outstanding replies first: a refresh that revoked a
                    // partition must not race the completion that would advance it.
                    if (!drainInFlight()) {
                        return;
                    }
                    // Re-read the clock: the drain can take longer than the refresh
                    // interval, and dating the next deadline from before it would put
                    // that deadline in the past and refresh again immediately.
                    refreshAssignment(System.nanoTime());
                }
                if (owned.isEmpty()) {
                    LockSupport.parkNanos(ERROR_BACKOFF_NANOS);
                    continue;
                }
                maintainCommits(false);
                issuePolls();
                if (!awaitCompletion()) {
                    return;
                }
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

    /**
     * Issues polls for eligible partitions until the concurrency budget is spent.
     *
     * <p>A partition is eligible when it is owned, has no poll outstanding, and its backoff has
     * expired. Backoff is per partition: one partition returning empty must not hold back another
     * that has messages waiting, which is what a single shared empty-round counter did.
     *
     * <p>The sweep resumes after the partition it issued last. Starting from the front every time
     * would be strict priority by index, and since a partition with messages is eligible again the
     * moment its reply lands, a busy low-index partition would take the whole budget forever and the
     * rest would never be polled at all.
     */
    private void issuePolls() {
        long now = System.nanoTime();
        int size = owned.size();
        int start = sweepStart;
        for (int offset = 0; offset < size; offset++) {
            if (inFlightPolls.size() >= pollConcurrency) {
                return;
            }
            int index = (start + offset) % size;
            long partition = owned.get(index);
            Cursor cursor = cursors.get(partition);
            if (cursor == null
                    || inFlightPolls.containsKey(partition)
                    || now - cursor.eligibleAtNanos < 0) {
                continue;
            }
            issuePoll(partition, cursor);
            sweepStart = index + 1;
        }
    }

    private void issuePoll(long partition, Cursor cursor) {
        long generation = assignmentGeneration;
        long requestId = nextPollId++;
        inFlightPolls.put(partition, requestId);
        CompletableFuture<PolledMessages> poll;
        try {
            poll =
                    client
                            .messages()
                            .pollMessages(
                                    streamId,
                                    topicId,
                                    Optional.of(partition),
                                    consumer,
                                    PollingStrategy.offset(BigInteger.valueOf(cursor.next)),
                                    pollSize,
                                    false);
        } catch (RuntimeException e) {
            enqueueCompletion(new Completion(generation, requestId, partition, null, e));
            return;
        }
        poll.whenComplete(
                (polled, error) ->
                        enqueueCompletion(new Completion(generation, requestId, partition, polled, error)));
    }

    private void enqueueCompletion(Completion completion) {
        synchronized (completionLock) {
            if (closed.get()) {
                return;
            }
            if (!completions.offer(completion)) {
                closed.set(true);
                pollThread.interrupt();
                log.error("Consumer completion queue exceeded its admitted poll count");
            }
        }
    }

    /**
     * Waits for one reply and applies it, or returns false when the consumer is closing.
     *
     * <p>Parks only when nothing is outstanding and nothing is eligible, so an idle topic still backs
     * off while a busy one never does.
     *
     * @return false when the consumer is closing and the poll loop should stop
     */
    private boolean awaitCompletion() throws InterruptedException {
        Completion completion = completions.poll(IDLE_BACKOFF_MILLIS, TimeUnit.MILLISECONDS);
        if (completion == null) {
            return !closed.get();
        }
        applyCompletion(completion);
        // Apply whatever else already arrived before going back for more, so a
        // burst of replies does not pay a loop iteration each.
        while ((completion = completions.poll()) != null) {
            applyCompletion(completion);
        }
        return !closed.get();
    }

    private void applyCompletion(Completion completion) {
        Long active = inFlightPolls.get(completion.partition);
        if (active == null || active.longValue() != completion.requestId) {
            return;
        }
        inFlightPolls.remove(completion.partition);
        if (closed.get()) {
            return;
        }
        Cursor cursor = cursors.get(completion.partition);
        // A reply from before a rebalance describes a partition this member may
        // no longer own, so it cannot deliver or move a cursor.
        if (completion.generation != assignmentGeneration || cursor == null) {
            return;
        }
        if (completion.error != null) {
            if (!closed.get()) {
                logPollFailure(completion.error);
                invalidateAssignment();
            }
            cursor.eligibleAtNanos = System.nanoTime() + ERROR_BACKOFF_NANOS;
            return;
        }
        PolledMessages polled = completion.polled;
        if (polled.messages().isEmpty()) {
            if (polled.partitionId() != null
                    && polled.partitionId() == RESYNC_REQUIRED_PARTITION_SENTINEL) {
                // The server fenced the poll: the assignment moved. Re-sync before
                // polling this partition again.
                invalidateAssignment();
                cursor.eligibleAtNanos = System.nanoTime() + ERROR_BACKOFF_NANOS;
                return;
            }
            cursor.eligibleAtNanos = System.nanoTime() + IDLE_BACKOFF_NANOS;
            return;
        }
        cursor.eligibleAtNanos = System.nanoTime();
        for (Message message : polled.messages()) {
            if (closed.get()) {
                break;
            }
            long offset = message.header().offset().longValueExact();
            if (offset < cursor.next) {
                continue;
            }
            callback.messageReceived(
                    message.payload(), message.header().originTimestamp().longValue() / 1_000L);
            cursor.next = Math.addExact(offset, 1);
            cursor.dirty = true;
        }
        if (!closed.get()) {
            maybeCommit(completion.partition, cursor, false);
        }
    }

    /**
     * Waits for every outstanding poll to report back, applying each as it lands.
     *
     * <p>Called before an assignment refresh so no completion arrives after the cursors it refers to
     * have been replaced or dropped. The replies are applied rather than dropped because they were
     * issued under the assignment still in force, so their messages are this member's to deliver and
     * their progress is its to commit.
     *
     * @return false when the wait was interrupted
     */
    private boolean drainInFlight() {
        long deadline = System.nanoTime() + DRAIN_TIMEOUT_NANOS;
        while (!inFlightPolls.isEmpty() && System.nanoTime() - deadline < 0) {
            Completion completion;
            try {
                completion = completions.poll(IDLE_BACKOFF_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (completion != null) {
                applyCompletion(completion);
            }
        }
        if (!inFlightPolls.isEmpty()) {
            log.warn(
                    "Consumer group {} on {}/{}: {} polls did not report back within the drain budget",
                    groupId,
                    streamId,
                    topicId,
                    inFlightPolls.size());
            // Giving up on a reply frees its partition to be polled again, so
            // the abandoned one must not be applied if it lands later: its
            // cursor has moved on and applying it would rewind or redeliver.
            // The refresh that follows may keep the same assignment, so it
            // cannot be relied on to bump this.
            invalidateAssignment();
        }
        return true;
    }

    private void invalidateAssignment() {
        assignmentGeneration++;
        assignmentStale = true;
    }

    // Hands every message to the benchmark and returns the offset of the last one.
    private long deliver(PolledMessages polled) {
        long lastOffset = -1;
        for (Message message : polled.messages()) {
            if (closed.get()) {
                break;
            }
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
        if (!drainCommits(System.nanoTime() + DRAIN_TIMEOUT_NANOS)) {
            throw new TimeoutException(
                    "Consumer offset commits did not settle before assignment refresh");
        }
        Optional<ConsumerGroupAssignment> assignment =
                client.consumerGroups().syncConsumerGroup(streamId, topicId, groupId).get();
        List<Long> mine = assignment.map(a -> List.copyOf(a.partitions())).orElse(List.of());
        for (Map.Entry<Long, Cursor> entry : cursors.entrySet()) {
            if (!mine.contains(entry.getKey())) {
                maybeCommit(entry.getKey(), entry.getValue(), true);
            }
        }
        if (!drainCommits(System.nanoTime() + DRAIN_TIMEOUT_NANOS)) {
            throw new TimeoutException("Revoked consumer offsets did not settle");
        }
        for (Iterator<Map.Entry<Long, Cursor>> it = cursors.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, Cursor> entry = it.next();
            if (!mine.contains(entry.getKey())) {
                it.remove();
            }
        }
        for (long partition : mine) {
            if (!cursors.containsKey(partition)) {
                Cursor cursor = new Cursor();
                cursor.next = storedOffset(partition).map(offset -> Math.addExact(offset, 1)).orElse(0L);
                // The first commit of a fresh cursor is not held back by the interval.
                cursor.lastCommitNanos = now - commitIntervalNanos;
                cursors.put(partition, cursor);
            }
        }
        Long generation = assignment.map(ConsumerGroupAssignment::generation).orElse(null);
        if (!mine.equals(owned) || !java.util.Objects.equals(generation, coordinatorGeneration)) {
            // Every poll issued under the old set describes an assignment that
            // no longer holds, so bump the generation gating their replies.
            assignmentGeneration++;
            log.info(
                    "Consumer group {} on {}/{}: this member owns partitions {} (generation {})",
                    groupId,
                    streamId,
                    topicId,
                    mine,
                    assignment.map(ConsumerGroupAssignment::generation).orElse(-1L));
        }
        owned = mine;
        coordinatorGeneration = generation;
        assignmentStale = mine.isEmpty();
        assignmentRefreshDeadlineNanos = System.nanoTime() + ASSIGNMENT_REFRESH_NANOS;
    }

    private Optional<Long> storedOffset(long partition) throws Exception {
        try {
            Optional<ConsumerOffsetInfo> info =
                    client
                            .consumerOffsets()
                            .getConsumerOffset(streamId, topicId, Optional.of(partition), consumer)
                            .get();
            return info.map(ConsumerOffsetInfo::storedOffset).map(BigInteger::longValueExact);
        } catch (ExecutionException e) {
            if (IggyBenchmarkDriver.isNotFound(e)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private void completeCommit(long partition, Cursor cursor) {
        if (cursor.commit == null || !cursor.commit.isDone()) {
            return;
        }
        try {
            cursor.commit.join();
            cursor.acknowledged = Math.max(cursor.acknowledged, cursor.committingOffset);
            cursor.dirty = cursor.next - 1 > cursor.acknowledged;
        } catch (RuntimeException error) {
            cursor.dirty = true;
            cursor.commitEligibleNanos = System.nanoTime() + ERROR_BACKOFF_NANOS;
            logCommitFailure(partition, BigInteger.valueOf(cursor.committingOffset), error);
        }
        cursor.commit = null;
    }

    private void maybeCommit(long partition, Cursor cursor, boolean force) {
        completeCommit(partition, cursor);
        long now = System.nanoTime();
        if (cursor.commit != null
                || !cursor.dirty
                || now - cursor.commitEligibleNanos < 0
                || (!force
                        && commitIntervalNanos > 0
                        && now - cursor.lastCommitNanos < commitIntervalNanos)) {
            return;
        }
        cursor.committingOffset = cursor.next - 1;
        cursor.lastCommitNanos = now;
        try {
            cursor.commit =
                    client
                            .consumerOffsets()
                            .storeConsumerOffset(
                                    streamId,
                                    topicId,
                                    Optional.of(partition),
                                    consumer,
                                    BigInteger.valueOf(cursor.committingOffset));
        } catch (RuntimeException error) {
            cursor.commitEligibleNanos = System.nanoTime() + ERROR_BACKOFF_NANOS;
            logCommitFailure(partition, BigInteger.valueOf(cursor.committingOffset), error);
        }
    }

    private void maintainCommits(boolean force) {
        for (Map.Entry<Long, Cursor> entry : cursors.entrySet()) {
            maybeCommit(entry.getKey(), entry.getValue(), force);
        }
    }

    private boolean drainCommits(long deadline) {
        while (true) {
            boolean outstanding = false;
            for (Map.Entry<Long, Cursor> entry : cursors.entrySet()) {
                completeCommit(entry.getKey(), entry.getValue());
                outstanding |= entry.getValue().commit != null;
            }
            if (!outstanding) {
                return true;
            }
            if (Thread.currentThread().isInterrupted() || System.nanoTime() - deadline >= 0) {
                return false;
            }
            LockSupport.parkNanos(IDLE_BACKOFF_NANOS);
        }
    }

    private void logPollFailure(Throwable error) {
        log.warn(
                "Poll failed for {}/{} group {}: {}",
                streamId,
                topicId,
                groupId,
                IggyBenchmarkDriver.unwrap(error).toString());
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
        if (!closeStarted.compareAndSet(false, true)) {
            return;
        }
        closed.set(true);
        pollThread.interrupt();
        try {
            pollThread.join(TimeUnit.SECONDS.toMillis(CLOSE_TIMEOUT_SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!autoCommit) {
            if (pollThread.isAlive()) {
                // The poll thread owns the cursors and it is still mutating them.
                // Reading them here would race, and a torn offset is worse than a
                // missing one: the next run would replay from it.
                log.warn(
                        "Consumer group {} on {}/{}: poll thread did not stop, skipping the final offset store",
                        groupId,
                        streamId,
                        topicId);
            } else {
                // Replies still on the wire are abandoned, exactly as the blocking
                // poll abandoned its own. Their messages were never delivered, so
                // committing their offsets would skip them on the next run.
                for (Map.Entry<Long, Cursor> entry : new ArrayList<>(cursors.entrySet())) {
                    maybeCommit(entry.getKey(), entry.getValue(), true);
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CLOSE_TIMEOUT_SECONDS);
                while (System.nanoTime() - deadline < 0 && !Thread.currentThread().isInterrupted()) {
                    maintainCommits(true);
                    if (!drainCommits(deadline)) {
                        break;
                    }
                    if (cursors.values().stream().noneMatch(cursor -> cursor.dirty)) {
                        break;
                    }
                    LockSupport.parkNanos(IDLE_BACKOFF_NANOS);
                }
            }
        }
        synchronized (completionLock) {
            completions.clear();
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
        long acknowledged = -1;
        long committingOffset;
        long commitEligibleNanos = System.nanoTime();
        CompletableFuture<Void> commit;

        /**
         * When this partition may be polled again. Set after an empty or failed reply so one quiet
         * partition backs off alone instead of holding back every other partition's next poll.
         *
         * <p>Always a {@link System#nanoTime()} reading, never a sentinel: that clock's origin is
         * arbitrary and may be negative, so a literal would compare as a deadline in the future.
         */
        long eligibleAtNanos = System.nanoTime();
    }

    /**
     * One poll reply on its way back to the poll thread, tagged with the assignment generation it was
     * issued under. Exactly one of {@code polled} and {@code error} is set.
     */
    private static final class Completion {
        private final long generation;
        private final long requestId;
        private final long partition;
        private final PolledMessages polled;
        private final Throwable error;

        Completion(
                long generation, long requestId, long partition, PolledMessages polled, Throwable error) {
            this.generation = generation;
            this.requestId = requestId;
            this.partition = partition;
            this.polled = polled;
            this.error = error;
        }
    }
}
