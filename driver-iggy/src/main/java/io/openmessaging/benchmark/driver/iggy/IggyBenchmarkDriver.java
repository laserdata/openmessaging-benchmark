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


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.exception.IggyConflictException;
import org.apache.iggy.exception.IggyResourceNotFoundException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenMessaging Benchmark driver for Apache Iggy over the TCP transport.
 *
 * <p>One admin connection manages the stream and the topics. Every producer and every consumer gets
 * its own connection, because a VSR session (and with it consumer group membership) is bound to one
 * connection.
 */
public class IggyBenchmarkDriver implements BenchmarkDriver {

    private static final Logger log = LoggerFactory.getLogger(IggyBenchmarkDriver.class);

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final long CLOSE_TIMEOUT_SECONDS = 30;

    private IggyConfig config;
    private StreamId streamId;
    private AsyncIggyTcpClient admin;
    private ScheduledExecutorService lingerExecutor;

    private final List<String> createdTopics = Collections.synchronizedList(new ArrayList<>());
    private final List<IggyBenchmarkProducer> producers =
            Collections.synchronizedList(new ArrayList<>());
    private final List<IggyBenchmarkConsumer> consumers =
            Collections.synchronizedList(new ArrayList<>());

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = mapper.readValue(configurationFile, IggyConfig.class);
        streamId = StreamId.of(config.streamName);
        lingerExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "iggy-producer-linger");
                            thread.setDaemon(true);
                            return thread;
                        });
        admin = connect().join();
        ensureStream();
        log.info(
                "Iggy driver initialized: {}:{} stream={} batchSize={} batchBytes={} lingerMs={} pollSize={}",
                config.host,
                config.port,
                config.streamName,
                config.producerBatchSize,
                config.producerBatchBytes,
                config.producerLingerMs,
                config.consumerPollSize);
    }

    private CompletableFuture<AsyncIggyTcpClient> connect() {
        return AsyncIggyTcpClient.builder()
                .host(config.host)
                .port(config.port)
                .credentials(config.username, config.password)
                .buildAndLogin();
    }

    private void ensureStream() {
        Optional<StreamDetails> existing;
        try {
            existing = admin.streams().getStream(streamId).join();
        } catch (CompletionException e) {
            if (!isNotFound(e)) {
                throw e;
            }
            existing = Optional.empty();
        }
        if (existing.isPresent()) {
            log.info("Using existing stream {}", config.streamName);
            return;
        }
        try {
            admin.streams().createStream(config.streamName).join();
            log.info("Created stream {}", config.streamName);
        } catch (CompletionException e) {
            // Another worker may have created it in the meantime.
            if (!isConflict(e)) {
                throw e;
            }
            log.info("Stream {} was created concurrently", config.streamName);
        }
    }

    @Override
    public String getTopicNamePrefix() {
        return "test-topic";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        return admin
                .topics()
                .createTopic(
                        streamId,
                        (long) partitions,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        topic)
                .thenAccept(
                        details -> {
                            createdTopics.add(topic);
                            log.info("Created topic {} with {} partitions", topic, partitions);
                        });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        TopicId topicId = TopicId.of(topic);
        return connect()
                .thenCompose(
                        client ->
                                openProducer(client, topicId)
                                        .exceptionallyCompose(error -> closeAndFail(client, error)));
    }

    private CompletableFuture<BenchmarkProducer> openProducer(
            AsyncIggyTcpClient client, TopicId topicId) {
        return partitionsCount(client, topicId)
                .thenApply(
                        partitions -> {
                            IggyBenchmarkProducer producer =
                                    new IggyBenchmarkProducer(
                                            client, streamId, topicId, partitions, config, lingerExecutor);
                            producers.add(producer);
                            return producer;
                        });
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        TopicId topicId = TopicId.of(topic);
        ConsumerId groupId = ConsumerId.of(subscriptionName);
        return connect()
                .thenCompose(
                        client ->
                                openConsumer(client, topicId, groupId, consumerCallback)
                                        .exceptionallyCompose(error -> closeAndFail(client, error)));
    }

    private CompletableFuture<BenchmarkConsumer> openConsumer(
            AsyncIggyTcpClient client, TopicId topicId, ConsumerId groupId, ConsumerCallback callback) {
        return ensureConsumerGroup(client, topicId, groupId)
                .thenCompose(
                        ignored -> client.consumerGroups().joinConsumerGroup(streamId, topicId, groupId))
                .thenCompose(ignored -> partitionsCount(client, topicId))
                .thenApply(
                        partitions -> {
                            IggyBenchmarkConsumer consumer =
                                    new IggyBenchmarkConsumer(
                                            client,
                                            streamId,
                                            topicId,
                                            groupId,
                                            partitions,
                                            config.consumerPollSize,
                                            callback);
                            consumers.add(consumer);
                            return consumer;
                        });
    }

    private CompletableFuture<Void> ensureConsumerGroup(
            AsyncIggyTcpClient client, TopicId topicId, ConsumerId groupId) {
        return client
                .consumerGroups()
                .getConsumerGroup(streamId, topicId, groupId)
                .thenCompose(
                        existing -> {
                            if (existing.isPresent()) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return client
                                    .consumerGroups()
                                    .createConsumerGroup(streamId, topicId, groupId.getName())
                                    .handle(this::ignoreConflict);
                        });
    }

    // Sibling consumers of one subscription race to create the group; the loser tolerates it.
    private Void ignoreConflict(Object ignored, Throwable error) {
        if (error != null && !isConflict(error)) {
            throw new CompletionException(unwrap(error));
        }
        return null;
    }

    private CompletableFuture<Long> partitionsCount(AsyncIggyTcpClient client, TopicId topicId) {
        return client
                .topics()
                .getTopic(streamId, topicId)
                .thenApply(
                        details ->
                                details
                                        .map(TopicDetails::partitionsCount)
                                        .orElseThrow(() -> topicNotFound(topicId)));
    }

    private IllegalStateException topicNotFound(TopicId topicId) {
        return new IllegalStateException("Topic " + topicId + " not found in stream " + streamId);
    }

    private static <T> CompletableFuture<T> closeAndFail(AsyncIggyTcpClient client, Throwable error) {
        return client
                .close()
                .handle(
                        (ignored, closeError) -> {
                            throw new CompletionException(unwrap(error));
                        });
    }

    @Override
    public void close() throws Exception {
        for (IggyBenchmarkProducer producer : new ArrayList<>(producers)) {
            closeQuietly(producer, "producer");
        }
        for (IggyBenchmarkConsumer consumer : new ArrayList<>(consumers)) {
            closeQuietly(consumer, "consumer");
        }
        if (lingerExecutor != null) {
            lingerExecutor.shutdownNow();
        }
        if (admin == null) {
            return;
        }
        for (String topic : new ArrayList<>(createdTopics)) {
            awaitQuietly(
                    admin
                            .topics()
                            .deleteTopic(streamId, TopicId.of(topic))
                            .thenRun(() -> log.info("Deleted topic {}", topic)),
                    "deleting topic " + topic);
        }
        awaitQuietly(admin.close(), "closing admin connection");
    }

    /**
     * Waits for a shutdown step with a timeout. Failures are logged, not thrown, so the remaining
     * steps still run; an interrupt is restored and skips the waiting, not the step itself.
     *
     * @param future the step that has already been started
     * @param what description used in the log
     */
    static void awaitQuietly(CompletableFuture<?> future, String what) {
        try {
            future.get(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while {}", what);
        } catch (ExecutionException | TimeoutException e) {
            log.warn("Failed while {}: {}", what, unwrap(e).toString());
        }
    }

    private static void closeQuietly(AutoCloseable closeable, String what) {
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("Failed to close {}: {}", what, e.toString());
        }
    }

    static boolean isConflict(Throwable error) {
        return unwrap(error) instanceof IggyConflictException;
    }

    static boolean isNotFound(Throwable error) {
        return unwrap(error) instanceof IggyResourceNotFoundException;
    }

    static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
