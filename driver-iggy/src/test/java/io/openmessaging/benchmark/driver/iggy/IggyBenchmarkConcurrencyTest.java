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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iggy.client.async.ConsumerGroupsClient;
import org.apache.iggy.client.async.ConsumerOffsetsClient;
import org.apache.iggy.client.async.MessagesClient;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.SendMessagesResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(10)
class IggyBenchmarkConcurrencyTest {
    private static final StreamId STREAM = StreamId.of(1L);
    private static final TopicId TOPIC = TopicId.of(1L);

    @Test
    void producerTimerOnlyReadiesAndWraparoundDeadlineStillFires() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        fixture.config.producerBatchSize = 100;
        fixture.clock.set(Long.MAX_VALUE - 500_000);
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> message = producer.sendAsync(Optional.empty(), new byte[1]);
        assertEquals(1, fixture.timers.size());
        fixture.clock.addAndGet(1_000_000);
        fixture.timers.removeFirst().run();
        assertEquals(0, fixture.replies.size());
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[1]);
        assertTrue(fixture.timers.isEmpty());
        fixture.tasks.removeFirst().run();
        assertEquals(1, fixture.replies.size());
        assertEquals(List.of(List.of(0, 0)), fixture.batches);
        fixture.replies.get(0).complete(SendMessagesResponse.empty());
        CompletableFuture.allOf(message, second).get(1, TimeUnit.SECONDS);
        producer.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void expiredProducerBatchKeepsFillingUntilItsLimit(boolean byteLimit) throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        fixture.config.producerBatchSize = byteLimit ? 100 : 3;
        fixture.config.producerBatchBytes = byteLimit ? 3 : 1024 * 1024;
        fixture.config.producerMaxPendingBytes = 4096;
        IggyBenchmarkProducer producer = fixture.create();
        List<CompletableFuture<Void>> messages = new ArrayList<>();
        for (int index = 1; index <= 3; index++) {
            messages.add(producer.sendAsync(Optional.empty(), new byte[] {(byte) index}));
        }
        fixture.tasks.removeFirst().run();
        messages.add(producer.sendAsync(Optional.empty(), new byte[] {4}));
        fixture.clock.addAndGet(1_000_000);
        fixture.timers.removeFirst().run();
        assertTrue(fixture.tasks.isEmpty());
        for (int index = 5; index <= 6; index++) {
            messages.add(producer.sendAsync(Optional.empty(), new byte[] {(byte) index}));
        }
        assertTrue(fixture.timers.isEmpty());

        messages.add(producer.sendAsync(Optional.empty(), new byte[] {7}));
        fixture.replies.get(0).complete(SendMessagesResponse.empty());
        fixture.tasks.removeFirst().run();
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5, 6)), fixture.batches);
        messages.add(producer.sendAsync(Optional.empty(), new byte[] {8}));
        fixture.clock.addAndGet(1_000_000);
        fixture.timers.removeFirst().run();
        assertTrue(fixture.timers.isEmpty());
        fixture.replies.get(1).complete(SendMessagesResponse.empty());
        fixture.tasks.removeFirst().run();
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5, 6), List.of(7, 8)), fixture.batches);
        fixture.replies.get(2).complete(SendMessagesResponse.empty());
        CompletableFuture.allOf(messages.toArray(CompletableFuture[]::new)).get(1, TimeUnit.SECONDS);
        assertTrue(fixture.tasks.isEmpty());
        assertEquals(0L, field(producer, "pendingBytes"));
        producer.close();
    }

    @Test
    void closeSendsAnExpiredOpenBatchOnlyOnce() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        fixture.config.producerBatchSize = 100;
        fixture.config.producerMaxInFlightBatches = 0;
        fixture.closeTimeoutNanos = TimeUnit.SECONDS.toNanos(2);
        CountDownLatch closing = new CountDownLatch(1);
        ScheduledFuture<?> timer = mock(ScheduledFuture.class);
        when(timer.cancel(false))
                .thenAnswer(
                        call -> {
                            closing.countDown();
                            return true;
                        });
        doAnswer(
                        call -> {
                            fixture.timers.addLast(call.getArgument(0));
                            return timer;
                        })
                .when(fixture.scheduler)
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        fixture.clock.addAndGet(1_000_000);
        fixture.timers.removeFirst().run();
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        CompletableFuture<Void> third = producer.sendAsync(Optional.of("key"), new byte[] {3});
        var executor = Executors.newSingleThreadExecutor();
        try {
            var closed =
                    executor.submit(
                            () -> {
                                producer.close();
                                return null;
                            });
            assertTrue(closing.await(1, TimeUnit.SECONDS));
            fixture.tasks.removeFirst().run();
            assertEquals(List.of(List.of(1, 2), List.of(3)), fixture.batches);
            fixture.replies.forEach(reply -> reply.complete(SendMessagesResponse.empty()));
            closed.get(1, TimeUnit.SECONDS);
            CompletableFuture.allOf(first, second, third).get(1, TimeUnit.SECONDS);
            assertEquals(0L, field(producer, "pendingBytes"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void producerResumesAfterLastPermitAndKeepsBatchOrder() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        fixture.tasks.removeFirst().run();
        assertEquals(1, fixture.replies.size());
        assertTrue(fixture.tasks.isEmpty());
        fixture.replies.get(0).complete(SendMessagesResponse.empty());
        assertEquals(1, fixture.tasks.size());
        fixture.tasks.removeFirst().run();
        assertEquals(List.of(1, 2), fixture.payloads);
        fixture.replies.get(1).complete(SendMessagesResponse.empty());
        CompletableFuture.allOf(first, second).get(1, TimeUnit.SECONDS);
        producer.close();
    }

    @Test
    void producerCanEnqueueFromCompletionWithoutLosingTheWakeup() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> next =
                first.thenCompose(ignored -> producer.sendAsync(Optional.empty(), new byte[] {2}));
        fixture.tasks.removeFirst().run();
        fixture.replies.get(0).complete(SendMessagesResponse.empty());
        fixture.tasks.removeFirst().run();
        fixture.replies.get(1).complete(SendMessagesResponse.empty());
        next.get(1, TimeUnit.SECONDS);
        producer.close();
    }

    @Test
    void producerSettlesSynchronousErrorAndReleasesItsPermit() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        when(fixture.messages.sendMessages(any(StreamId.class), any(TopicId.class), any(), anyList()))
                .thenThrow(new AssertionError("encoding failed"))
                .thenReturn(CompletableFuture.completedFuture(SendMessagesResponse.empty()));
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[1]);
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[1]);
        fixture.tasks.removeFirst().run();
        assertTrue(first.isCompletedExceptionally());
        second.get(1, TimeUnit.SECONDS);
        assertEquals(0L, field(producer, "pendingBytes"));
        producer.close();
    }

    @Test
    void rejectedProducerLaneFailsOpenAndReadyBatches() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        fixture.config.producerBatchSize = 2;
        fixture.executor =
                ignored -> {
                    throw new RejectedExecutionException("stopped");
                };
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[1]);
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[1]);
        assertTrue(first.isCompletedExceptionally());
        assertTrue(second.isCompletedExceptionally());
        CompletionException closed =
                assertThrows(
                        CompletionException.class,
                        () -> producer.sendAsync(Optional.empty(), new byte[1]).join());
        assertEquals("stopped", closed.getCause().getCause().getMessage());
        assertEquals(0L, field(producer, "pendingBytes"));
        producer.close();
    }

    @Test
    void rejectedLingerFailsTheAcceptedMessageInsteadOfParkingIt() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        fixture.config.producerBatchSize = 100;
        when(fixture.scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenThrow(new RejectedExecutionException("stopped"));
        IggyBenchmarkProducer producer = fixture.create();
        assertTrue(producer.sendAsync(Optional.empty(), new byte[1]).isCompletedExceptionally());
        assertEquals(0L, field(producer, "pendingBytes"));
        producer.close();
    }

    @Test
    void cleanProducerCloseDoesNotClaimAFatalError() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        IggyBenchmarkProducer producer = fixture.create();
        producer.close();
        CompletionException closed =
                assertThrows(
                        CompletionException.class,
                        () -> producer.sendAsync(Optional.empty(), new byte[1]).join());
        assertEquals("Producer is closed", closed.getCause().getMessage());
        assertEquals(null, closed.getCause().getCause());
    }

    @Test
    void producerYieldsItsWorkerWithAnUnlimitedInflightWindow() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        fixture.config.producerMaxInFlightBatches = 0;
        fixture.config.producerMaxPendingBytes = 0;
        IggyBenchmarkProducer producer = fixture.create();
        for (int index = 0; index < 100; index++) {
            producer.sendAsync(Optional.empty(), new byte[1]);
        }
        fixture.tasks.removeFirst().run();
        assertTrue(fixture.replies.size() < 100);
        assertEquals(1, fixture.tasks.size());
        fixture.tasks.removeFirst().run();
        assertEquals(100, fixture.replies.size());
        fixture.replies.forEach(reply -> reply.complete(SendMessagesResponse.empty()));
        producer.close();
    }

    @Test
    void closeNeverBecomesASecondSenderAndSettlesLateReplyOnlyOnce() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[1]);
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[1]);
        fixture.tasks.removeFirst().run();
        producer.close();
        assertEquals(1, fixture.replies.size());
        assertTrue(first.isCompletedExceptionally());
        assertTrue(second.isCompletedExceptionally());
        fixture.replies.get(0).complete(SendMessagesResponse.empty());
        assertEquals(0L, field(producer, "pendingBytes"));
        assertTrue(first.isCompletedExceptionally());
    }

    @Test
    void zeroPayloadStillConsumesBudgetAndCloseReleasesBlockedLoadThread() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        fixture.config.producerMaxPendingBytes = MessageHeader.SIZE;
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[0]);
        CountDownLatch entered = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var blocked =
                    executor.submit(
                            () -> {
                                entered.countDown();
                                return producer.sendAsync(Optional.empty(), new byte[0]);
                            });
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            producer.close();
            assertTrue(blocked.get(1, TimeUnit.SECONDS).isCompletedExceptionally());
            assertTrue(first.isCompletedExceptionally());
            assertEquals(0L, field(producer, "pendingBytes"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void closeDuringSynchronousSubmissionDoesNotInterleaveAnotherSend() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<SendMessagesResponse> reply = new CompletableFuture<>();
        AtomicLong submissions = new AtomicLong();
        var executor = Executors.newSingleThreadExecutor();
        fixture.executor = executor;
        when(fixture.messages.sendMessages(any(StreamId.class), any(TopicId.class), any(), anyList()))
                .thenAnswer(
                        call -> {
                            submissions.incrementAndGet();
                            entered.countDown();
                            assertTrue(release.await(2, TimeUnit.SECONDS));
                            return reply;
                        });
        IggyBenchmarkProducer producer = fixture.create();
        try {
            CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[1]);
            CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[1]);
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            producer.close();
            assertTrue(first.isCompletedExceptionally());
            assertTrue(second.isCompletedExceptionally());
            release.countDown();
            reply.complete(SendMessagesResponse.empty());
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            assertEquals(1, submissions.get());
            assertEquals(0L, field(producer, "pendingBytes"));
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void oldestProducerDeadlineIsNotPostponedByLaterMessages() throws Exception {
        ProducerFixture fixture = new ProducerFixture();
        fixture.config.producerBatchSize = 100;
        IggyBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[1]);
        fixture.clock.addAndGet(500_000);
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[1]);
        assertEquals(1, fixture.timers.size());
        fixture.clock.addAndGet(500_000);
        fixture.timers.removeFirst().run();
        fixture.tasks.removeFirst().run();
        assertEquals(1, fixture.replies.size());
        fixture.replies.get(0).complete(SendMessagesResponse.empty());
        CompletableFuture.allOf(first, second).get(1, TimeUnit.SECONDS);
        producer.close();
    }

    @Test
    void derivedBudgetOverflowIsRejected() {
        IggyConfig config = new IggyConfig();
        config.producerBatchBytes = Long.MAX_VALUE;
        config.producerMaxInFlightBatches = 2;
        assertThrows(IllegalArgumentException.class, config::resolvedMaxPendingBytes);
    }

    @Test
    void derivedBudgetIncludesEveryConfiguredMessageHeader() {
        IggyConfig config = new IggyConfig();
        config.producerBatchBytes = 1024 * 1024;
        config.producerBatchSize = 1000;
        config.producerMaxInFlightBatches = 16;
        long chargedBatchBytes = config.producerBatchBytes + 1000L * MessageHeader.SIZE;
        assertEquals(chargedBatchBytes * 16, config.resolvedMaxPendingBytes());
    }

    @Test
    void autoCommitAlwaysUsesOneEffectivePoll() {
        IggyConfig config = new IggyConfig();
        config.consumerPollConcurrency = 8;
        assertEquals(1, config.effectiveConsumerPollConcurrency());
        config.consumerAutoCommit = false;
        assertEquals(8, config.effectiveConsumerPollConcurrency());
    }

    @Test
    void notOwnedIsRecognisedByItsRawCodeThroughCompletionWrappers() {
        assertTrue(
                IggyBenchmarkDriver.isNotOwned(new CompletionException(new IggyServerException(5009))));
        assertTrue(!IggyBenchmarkDriver.isNotOwned(new IggyServerException(3024)));
        assertTrue(!IggyBenchmarkDriver.isNotOwned(new IllegalStateException("not owned")));
    }

    @Test
    void concurrentSweepVisitsConsecutivePartitionsAndThenRotates() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(2, List.of(0L, 1L, 2L, 3L));
        fixture.refresh();
        invoke(fixture.consumer, "issuePolls");
        assertEquals(List.of(0L, 1L), fixture.polledPartitions);
        fixture.polls.get(0).complete(polled(0, 0));
        fixture.polls.get(1).complete(polled(1, 0));
        invoke(fixture.consumer, "awaitCompletion");
        invoke(fixture.consumer, "issuePolls");
        assertEquals(List.of(0L, 1L, 2L, 3L), fixture.polledPartitions);
    }

    @Test
    void invalidatedPollRetainsItsSlotUntilCompletionAndNeverDelivers() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L));
        fixture.refresh();
        invoke(fixture.consumer, "issuePolls");
        invoke(fixture.consumer, "invalidateAssignment");
        invoke(fixture.consumer, "issuePolls");
        assertEquals(1, fixture.polls.size());
        fixture.polls.get(0).complete(polled(0, 9));
        invoke(fixture.consumer, "awaitCompletion");
        assertTrue(fixture.delivered.isEmpty());
        invoke(fixture.consumer, "issuePolls");
        assertEquals(2, fixture.polls.size());
        fixture.polls.get(1).complete(polled(0, 0));
        invoke(fixture.consumer, "awaitCompletion");
        assertEquals(List.of(0), fixture.delivered);
    }

    @Test
    void synchronousPollFailureKeepsItsSlotUntilTheErrorIsProcessed() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L, 1L));
        fixture.refresh();
        when(fixture.messages.pollMessages(
                        any(StreamId.class), any(TopicId.class), any(), any(), any(), anyLong(), anyBoolean()))
                .thenThrow(new IllegalStateException("poll failed"));
        invoke(fixture.consumer, "issuePolls");
        assertEquals(1, ((Map<?, ?>) field(fixture.consumer, "inFlightPolls")).size());
        assertEquals(1, ((java.util.Queue<?>) field(fixture.consumer, "completions")).size());
        invoke(fixture.consumer, "awaitCompletion");
        assertTrue(((Map<?, ?>) field(fixture.consumer, "inFlightPolls")).isEmpty());
    }

    @Test
    void authoritativeGenerationChangesEvenWithIdenticalPartitionAssignment() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L));
        fixture.refresh();
        long generation = (long) field(fixture.consumer, "assignmentGeneration");
        fixture.generation++;
        fixture.refresh();
        assertEquals(generation + 1, field(fixture.consumer, "assignmentGeneration"));
    }

    @Test
    void commitsSerializeAndRetryDirtyProgressWithoutAnotherMessage() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L));
        fixture.refresh();
        fixture.read(0);
        fixture.read(1);
        assertEquals(List.of(BigInteger.ZERO), fixture.storedOffsets);
        fixture.commits.get(0).complete(null);
        invoke(fixture.consumer, "maintainCommits", false);
        assertEquals(List.of(BigInteger.ZERO, BigInteger.ONE), fixture.storedOffsets);
        fixture.commits.get(1).completeExceptionally(new IllegalStateException("store failed"));
        invoke(fixture.consumer, "maintainCommits", false);
        Object cursor = ((Map<?, ?>) field(fixture.consumer, "cursors")).get(0L);
        setField(cursor, "commitEligibleNanos", System.nanoTime() - 1);
        invoke(fixture.consumer, "maintainCommits", false);
        assertEquals(List.of(BigInteger.ZERO, BigInteger.ONE, BigInteger.ONE), fixture.storedOffsets);
        fixture.commits.get(2).complete(null);
        invoke(fixture.consumer, "maintainCommits", false);
        assertEquals(false, field(cursor, "dirty"));
    }

    @Test
    void assignmentRefreshDoesNotWaitForCommitOnRetainedPartition() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L));
        fixture.refresh();
        fixture.read(0);
        assertTrue(!fixture.commits.get(0).isDone());
        CountDownLatch synced = new CountDownLatch(1);
        fixture.onSync = synced::countDown;
        var executor = Executors.newSingleThreadExecutor();
        try {
            var refreshed =
                    executor.submit(
                            () -> {
                                fixture.refresh();
                                return null;
                            });
            assertTrue(synced.await(1, TimeUnit.SECONDS));
            refreshed.get(1, TimeUnit.SECONDS);
            assertEquals(1, fixture.commits.size());
        } finally {
            fixture.commits.forEach(commit -> commit.complete(null));
            executor.shutdownNow();
        }
    }

    @Test
    void assignmentRefreshSettlesOnlyRevokedCursorAtItsLatestOffset() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L, 1L));
        fixture.refresh();
        Map<?, ?> cursors = (Map<?, ?>) field(fixture.consumer, "cursors");
        Object retained = cursors.get(0L);
        Object revoked = cursors.get(1L);
        setField(retained, "next", 1L);
        setField(retained, "dirty", true);
        setField(revoked, "next", 1L);
        setField(revoked, "dirty", true);
        invoke(fixture.consumer, "maybeCommit", 0L, retained, false);
        invoke(fixture.consumer, "maybeCommit", 1L, revoked, false);
        setField(revoked, "next", 2L);
        fixture.assignedPartitions = List.of(0L);
        fixture.completeNewCommits = true;
        CountDownLatch synced = new CountDownLatch(1);
        fixture.onSync = synced::countDown;
        var executor = Executors.newSingleThreadExecutor();
        try {
            var refreshed =
                    executor.submit(
                            () -> {
                                fixture.refresh();
                                return null;
                            });
            assertTrue(synced.await(1, TimeUnit.SECONDS));
            assertTrue(!fixture.commits.get(0).isDone());
            fixture.commits.get(1).complete(null);
            refreshed.get(1, TimeUnit.SECONDS);
            assertEquals(
                    List.of(BigInteger.ZERO, BigInteger.ZERO, BigInteger.ONE), fixture.storedOffsets);
            assertEquals(List.of(0L), new ArrayList<>(cursors.keySet()));
            assertTrue(!fixture.commits.get(0).isDone());
        } finally {
            fixture.commits.forEach(commit -> commit.complete(null));
            executor.shutdownNow();
        }
    }

    @Test
    void assignmentRefreshRetriesFailedCommitBeforeRemovingRevokedCursor() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L));
        fixture.refresh();
        Object cursor = ((Map<?, ?>) field(fixture.consumer, "cursors")).get(0L);
        setField(cursor, "next", 1L);
        setField(cursor, "dirty", true);
        invoke(fixture.consumer, "maybeCommit", 0L, cursor, false);
        setField(cursor, "next", 2L);
        fixture.commits.get(0).completeExceptionally(new IllegalStateException("store failed"));
        fixture.assignedPartitions = List.of();
        fixture.completeNewCommits = true;

        fixture.refresh();

        assertEquals(List.of(BigInteger.ZERO, BigInteger.ONE), fixture.storedOffsets);
        assertTrue(((Map<?, ?>) field(fixture.consumer, "cursors")).isEmpty());
    }

    @Test
    void assignmentRefreshDropsRevokedCursorWhoseOffsetDoesNotSettle() throws Exception {
        ConsumerFixture fixture =
                new ConsumerFixture(1, List.of(0L, 1L), TimeUnit.MILLISECONDS.toNanos(50));
        fixture.refresh();
        Map<?, ?> cursors = (Map<?, ?>) field(fixture.consumer, "cursors");
        Object revoked = cursors.get(1L);
        setField(revoked, "next", 1L);
        setField(revoked, "dirty", true);
        invoke(fixture.consumer, "maybeCommit", 1L, revoked, false);
        fixture.assignedPartitions = List.of(0L);

        fixture.refresh();

        assertEquals(List.of(BigInteger.ZERO), fixture.storedOffsets);
        assertEquals(List.of(0L), new ArrayList<>(cursors.keySet()));
        assertEquals(List.of(0L), field(fixture.consumer, "owned"));
        assertTrue(!fixture.commits.get(0).isDone());
    }

    @Test
    void refusedOffsetStoreFencesTheCursorAndReseedsItFromTheStoredOffset() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L));
        fixture.refresh();
        Map<?, ?> cursors = (Map<?, ?>) field(fixture.consumer, "cursors");
        Object cursor = cursors.get(0L);
        setField(cursor, "next", 1L);
        setField(cursor, "dirty", true);
        invoke(fixture.consumer, "maybeCommit", 0L, cursor, false);
        setField(cursor, "next", 2L);
        fixture.commits.get(0).completeExceptionally(new IggyServerException(5009));
        fixture.completeNewCommits = true;

        invoke(fixture.consumer, "maintainCommits", false);
        assertEquals(true, field(cursor, "fenced"));
        assertEquals(true, field(fixture.consumer, "assignmentStale"));
        fixture.refresh();

        assertEquals(List.of(BigInteger.ZERO), fixture.storedOffsets);
        Object reseeded = cursors.get(0L);
        assertTrue(reseeded != cursor);
        assertEquals(0L, field(reseeded, "next"));
        assertEquals(false, field(reseeded, "dirty"));
    }

    @Test
    void completionAfterCloseNeitherDeliversNorAccumulatesInTheQueue() throws Exception {
        ConsumerFixture fixture = new ConsumerFixture(1, List.of(0L));
        fixture.refresh();
        invoke(fixture.consumer, "issuePolls");
        fixture.consumer.close();
        fixture.polls.get(0).complete(polled(0, 0));
        assertTrue(fixture.delivered.isEmpty());
        assertTrue(((java.util.Queue<?>) field(fixture.consumer, "completions")).isEmpty());
    }

    private static PolledMessages polled(long partition, long offset) {
        MessageHeader header =
                new MessageHeader(
                        BigInteger.ZERO,
                        MessageId.serverGenerated(),
                        BigInteger.valueOf(offset),
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        0L,
                        1L,
                        BigInteger.ZERO);
        Message message = Message.of(header, new byte[] {(byte) offset}, null);
        return new PolledMessages(partition, BigInteger.valueOf(offset), 1L, List.of(message));
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object invoke(Object target, String name, Object... arguments) throws Exception {
        for (Method method : target.getClass().getDeclaredMethods()) {
            if (method.getName().equals(name) && method.getParameterCount() == arguments.length) {
                method.setAccessible(true);
                return method.invoke(target, arguments);
            }
        }
        throw new NoSuchMethodException(name);
    }

    private static final class ProducerFixture {
        final AsyncIggyTcpClient client = mock(AsyncIggyTcpClient.class);
        final MessagesClient messages = mock(MessagesClient.class);
        final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        final IggyConfig config = new IggyConfig();
        final AtomicLong clock = new AtomicLong();
        final Deque<Runnable> timers = new ArrayDeque<>();
        final Deque<Runnable> tasks = new ArrayDeque<>();
        final List<CompletableFuture<SendMessagesResponse>> replies = new ArrayList<>();
        final List<Integer> payloads = new ArrayList<>();
        final List<List<Integer>> batches = new ArrayList<>();
        Executor executor = tasks::addLast;
        long closeTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(5);

        ProducerFixture() {
            config.producerBatchSize = 1;
            config.producerMaxInFlightBatches = 1;
            when(client.messages()).thenReturn(messages);
            when(client.close()).thenReturn(CompletableFuture.completedFuture(null));
            when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(
                            call -> {
                                timers.addLast(call.getArgument(0));
                                return mock(ScheduledFuture.class);
                            });
            when(messages.sendMessages(
                            any(StreamId.class), any(TopicId.class), any(Partitioning.class), anyList()))
                    .thenAnswer(
                            call -> {
                                List<Message> batch = call.getArgument(3);
                                byte[] payload = batch.get(0).payload();
                                payloads.add(payload.length == 0 ? 0 : (int) payload[0]);
                                batches.add(
                                        batch.stream()
                                                .map(Message::payload)
                                                .map(data -> data.length == 0 ? 0 : (int) data[0])
                                                .toList());
                                CompletableFuture<SendMessagesResponse> reply = new CompletableFuture<>();
                                replies.add(reply);
                                return reply;
                            });
        }

        IggyBenchmarkProducer create() {
            return new IggyBenchmarkProducer(
                    client,
                    STREAM,
                    TOPIC,
                    4,
                    config,
                    new IggyBenchmarkProducer.Execution(scheduler, executor, clock::get, closeTimeoutNanos));
        }
    }

    private static final class ConsumerFixture {
        final AsyncIggyTcpClient client = mock(AsyncIggyTcpClient.class);
        final MessagesClient messages = mock(MessagesClient.class);
        final ConsumerGroupsClient groups = mock(ConsumerGroupsClient.class);
        final ConsumerOffsetsClient offsets = mock(ConsumerOffsetsClient.class);
        final IggyBenchmarkConsumer consumer;
        final List<Long> polledPartitions = new ArrayList<>();
        final List<CompletableFuture<PolledMessages>> polls = new ArrayList<>();
        final List<BigInteger> storedOffsets = new ArrayList<>();
        final List<CompletableFuture<Void>> commits = new ArrayList<>();
        final List<Integer> delivered = new ArrayList<>();
        List<Long> assignedPartitions;
        Runnable onSync = () -> {};
        boolean completeNewCommits;
        long generation;

        ConsumerFixture(int concurrency, List<Long> partitions) {
            this(concurrency, partitions, TimeUnit.SECONDS.toNanos(10));
        }

        ConsumerFixture(int concurrency, List<Long> partitions, long settleTimeoutNanos) {
            assignedPartitions = partitions;
            IggyConfig config = new IggyConfig();
            config.consumerPollConcurrency = concurrency;
            config.consumerAutoCommit = false;
            when(client.messages()).thenReturn(messages);
            when(client.consumerGroups()).thenReturn(groups);
            when(client.consumerOffsets()).thenReturn(offsets);
            when(client.close()).thenReturn(CompletableFuture.completedFuture(null));
            when(groups.leaveConsumerGroup(any(), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(groups.syncConsumerGroup(any(), any(), any()))
                    .thenAnswer(
                            call -> {
                                onSync.run();
                                return CompletableFuture.completedFuture(
                                        Optional.of(new ConsumerGroupAssignment(generation, assignedPartitions)));
                            });
            when(offsets.getConsumerOffset(any(StreamId.class), any(TopicId.class), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
            when(offsets.storeConsumerOffset(
                            any(StreamId.class), any(TopicId.class), any(), any(), any()))
                    .thenAnswer(
                            call -> {
                                storedOffsets.add(call.getArgument(4));
                                CompletableFuture<Void> commit = new CompletableFuture<>();
                                commits.add(commit);
                                if (completeNewCommits) {
                                    commit.complete(null);
                                }
                                return commit;
                            });
            when(messages.pollMessages(
                            any(StreamId.class),
                            any(TopicId.class),
                            any(),
                            any(),
                            any(),
                            anyLong(),
                            anyBoolean()))
                    .thenAnswer(
                            call -> {
                                Optional<Long> partition = call.getArgument(2);
                                polledPartitions.add(partition.orElseThrow());
                                CompletableFuture<PolledMessages> poll = new CompletableFuture<>();
                                polls.add(poll);
                                return poll;
                            });
            ConsumerCallback callback = mock(ConsumerCallback.class);
            doAnswer(
                            call -> {
                                byte[] payload = call.getArgument(0);
                                delivered.add((int) payload[0]);
                                return null;
                            })
                    .when(callback)
                    .messageReceived(any(byte[].class), anyLong());
            consumer =
                    new IggyBenchmarkConsumer(
                            client,
                            STREAM,
                            TOPIC,
                            ConsumerId.of(1L),
                            partitions.size(),
                            config,
                            callback,
                            settleTimeoutNanos);
        }

        void refresh() throws Exception {
            invoke(consumer, "refreshAssignment", System.nanoTime());
        }

        void read(long offset) throws Exception {
            invoke(consumer, "issuePolls");
            polls.get(polls.size() - 1).complete(polled(0, offset));
            invoke(consumer, "awaitCompletion");
        }
    }
}
