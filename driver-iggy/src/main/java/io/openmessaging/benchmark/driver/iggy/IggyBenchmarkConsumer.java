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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer that polls its consumer group on a dedicated thread.
 *
 * <p>The connection has already joined the group when the consumer is constructed. Each poll uses
 * {@code PollingStrategy.next()} with auto-commit and serves one partition of the member's
 * assignment, so an empty poll does not mean the topic is drained: the loop backs off only after a
 * full empty cycle over the partitions. The callback runs on the poll thread, which is what lets
 * the worker pause consumers by blocking inside it.
 */
public class IggyBenchmarkConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(IggyBenchmarkConsumer.class);

    private static final long IDLE_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long ERROR_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long CLOSE_TIMEOUT_SECONDS = 30;

    private final AsyncIggyTcpClient client;
    private final StreamId streamId;
    private final TopicId topicId;
    private final ConsumerId groupId;
    private final Consumer consumer;
    private final long partitionsCount;
    private final long pollSize;
    private final ConsumerCallback callback;
    private final Thread pollThread;
    private final AtomicBoolean closed = new AtomicBoolean();

    IggyBenchmarkConsumer(
            AsyncIggyTcpClient client,
            StreamId streamId,
            TopicId topicId,
            ConsumerId groupId,
            long partitionsCount,
            int pollSize,
            ConsumerCallback callback) {
        this.client = client;
        this.streamId = streamId;
        this.topicId = topicId;
        this.groupId = groupId;
        this.consumer = Consumer.group(groupId);
        this.partitionsCount = Math.max(1, partitionsCount);
        this.pollSize = Math.max(1, pollSize);
        this.callback = callback;
        this.pollThread = new Thread(this::pollLoop, "iggy-consumer-" + topicId + "-" + groupId);
        this.pollThread.setDaemon(true);
        this.pollThread.start();
    }

    private void pollLoop() {
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
                for (Message message : polled.messages()) {
                    // originTimestamp is the producer's wall clock in microseconds.
                    long publishTimestampMs = message.header().originTimestamp().longValue() / 1_000L;
                    callback.messageReceived(message.payload(), publishTimestampMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (closed.get()) {
                    return;
                }
                log.warn(
                        "Poll failed for {}/{} group {}: {}",
                        streamId,
                        topicId,
                        groupId,
                        IggyBenchmarkDriver.unwrap(e).toString());
                LockSupport.parkNanos(ERROR_BACKOFF_NANOS);
            }
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
        IggyBenchmarkDriver.awaitQuietly(
                client.consumerGroups().leaveConsumerGroup(streamId, topicId, groupId),
                "leaving consumer group " + groupId);
        IggyBenchmarkDriver.awaitQuietly(client.close(), "closing consumer connection of " + groupId);
    }
}
