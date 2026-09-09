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
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientBuilder;
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyConflictException;
import org.apache.iggy.exception.IggyResourceNotFoundException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderValue;
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
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    private static final long CLOSE_TIMEOUT_SECONDS = 30;

    private IggyConfig config;
    private StreamId streamId;
    private List<Endpoint> endpoints;
    private Optional<RetryPolicy> retryPolicy;
    private Map<String, HeaderValue> topicOptions;
    private AsyncIggyTcpClient admin;
    private ScheduledExecutorService lingerExecutor;

    /** Shared by every connection; null when {@code ioThreads} is 0. */
    private IoEventLoopGroup ioGroup;

    private final AtomicInteger nextEndpoint = new AtomicInteger();
    private final List<String> createdTopics = Collections.synchronizedList(new ArrayList<>());
    private final List<IggyBenchmarkProducer> producers =
            Collections.synchronizedList(new ArrayList<>());
    private final List<IggyBenchmarkConsumer> consumers =
            Collections.synchronizedList(new ArrayList<>());

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = mapper.readValue(configurationFile, IggyConfig.class);
        streamId = StreamId.of(config.streamName);
        endpoints = endpoints(config);
        retryPolicy = retryPolicy(config);
        topicOptions = topicOptions(config);
        lingerExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "iggy-producer-linger");
                            thread.setDaemon(true);
                            return thread;
                        });
        ioGroup = ioGroup(config);
        admin = connect().join();
        ensureStream();
        log.info(
                "Iggy driver initialized: endpoints={} ioThreads={} stream={} connectionTimeoutMs={}"
                        + " requestTimeoutMs={} retryPolicy={} topicOptions={} batchSize={} batchBytes={}"
                        + " lingerMs={} maxInFlightBatches={} pollSize={}",
                endpoints,
                config.ioThreads,
                config.streamName,
                config.connectionTimeoutMs,
                config.requestTimeoutMs,
                config.retryPolicy,
                config.topicOptions,
                config.producerBatchSize,
                config.producerBatchBytes,
                config.producerLingerMs,
                config.producerMaxInFlightBatches,
                config.consumerPollSize);
    }

    /**
     * Builds the event loop group every connection registers on, or null for one loop per connection.
     * Callbacks run on these loops, so nothing in this driver may block inside a completion stage.
     *
     * @param config the driver settings
     * @return the shared group, or null when {@code ioThreads} is 0
     */
    static IoEventLoopGroup ioGroup(IggyConfig config) {
        if (config.ioThreads < 0) {
            throw new IllegalArgumentException(
                    "ioThreads must be 0 or positive, got " + config.ioThreads);
        }
        if (config.ioThreads == 0) {
            return null;
        }
        return new MultiThreadIoEventLoopGroup(
                config.ioThreads, new DefaultThreadFactory("iggy-io", true), NioIoHandler.newFactory());
    }

    static List<Endpoint> endpoints(IggyConfig config) {
        if (config.hosts == null || config.hosts.isEmpty()) {
            return List.of(new Endpoint(config.host, config.port));
        }
        List<Endpoint> result = new ArrayList<>();
        for (String address : config.hosts) {
            result.add(Endpoint.parse(address, config.port));
        }
        return Collections.unmodifiableList(result);
    }

    static Optional<RetryPolicy> retryPolicy(IggyConfig config) {
        String policy = config.retryPolicy == null ? "default" : config.retryPolicy;
        switch (policy.toLowerCase(Locale.ROOT)) {
            case "default":
                return Optional.empty();
            case "none":
                return Optional.of(RetryPolicy.noRetry());
            case "exponential":
                return Optional.of(RetryPolicy.exponentialBackoff());
            case "fixed":
                return Optional.of(
                        RetryPolicy.fixedDelay(config.retryMaxRetries, Duration.ofMillis(config.retryDelayMs)));
            default:
                throw new IllegalArgumentException(
                        "Unknown retryPolicy '"
                                + config.retryPolicy
                                + "', expected default, none, exponential or fixed");
        }
    }

    static Map<String, HeaderValue> topicOptions(IggyConfig config) {
        Map<String, HeaderValue> options = new LinkedHashMap<>();
        if (config.topicOptions != null) {
            config.topicOptions.forEach((key, value) -> options.put(key, HeaderValue.fromString(value)));
        }
        return Collections.unmodifiableMap(options);
    }

    private CompletableFuture<AsyncIggyTcpClient> connect() {
        int start = Math.floorMod(nextEndpoint.getAndIncrement(), endpoints.size());
        return connect(start, 1);
    }

    private CompletableFuture<AsyncIggyTcpClient> connect(int index, int attempt) {
        Endpoint endpoint = endpoints.get(index % endpoints.size());
        CompletableFuture<AsyncIggyTcpClient> login;
        try {
            login = login(endpoint);
        } catch (RuntimeException e) {
            login = CompletableFuture.failedFuture(e);
        }
        if (attempt >= endpoints.size()) {
            return login;
        }
        return login.exceptionallyCompose(
                error -> {
                    log.warn(
                            "Connecting to {} failed ({}), trying the next bootstrap address",
                            endpoint,
                            unwrap(error).toString());
                    return connect(index + 1, attempt + 1);
                });
    }

    private CompletableFuture<AsyncIggyTcpClient> login(Endpoint endpoint) {
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder()
                        .host(endpoint.host())
                        .port(endpoint.port())
                        .credentials(config.username, config.password)
                        .connectionTimeout(Duration.ofMillis(config.connectionTimeoutMs))
                        .requestTimeout(Duration.ofMillis(config.requestTimeoutMs));
        retryPolicy.ifPresent(builder::retryPolicy);
        if (ioGroup != null) {
            builder.eventLoopGroup(ioGroup);
        }
        return builder.buildAndLogin();
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

    /**
     * Creates the topic with the configured options. A key or value the server refuses fails the
     * future, and with it the run, which is the loud failure we want.
     */
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
                        topic,
                        topicOptions)
                .thenAccept(
                        details -> {
                            createdTopics.add(topic);
                            log.info(
                                    "Created topic {} with {} partitions, requested options {}, effective options {}",
                                    topic,
                                    partitions,
                                    config.topicOptions,
                                    effectiveOptions(details));
                        });
    }

    static Map<String, String> effectiveOptions(TopicDetails details) {
        Map<String, String> effective = new TreeMap<>();
        details.derivedOptions().forEach((key, value) -> effective.put(key, value.toStringValue()));
        details.options().forEach((key, value) -> effective.put(key, value.toStringValue()));
        return effective;
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
        if (admin != null) {
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
        if (ioGroup != null) {
            try {
                ioGroup
                        .shutdownGracefully(0, CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .await(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while shutting down the event loop group");
            }
        }
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

    /**
     * One bootstrap address.
     *
     * @param host host name or IP address
     * @param port TCP port
     */
    record Endpoint(String host, int port) {

        /**
         * Parses one {@code hosts} entry.
         *
         * @param address {@code host:port}, or a bare {@code host}
         * @param defaultPort port used when the entry has none
         * @return the endpoint
         */
        static Endpoint parse(String address, int defaultPort) {
            String trimmed = address == null ? "" : address.trim();
            int colon = trimmed.lastIndexOf(':');
            if (colon < 0) {
                return new Endpoint(trimmed, defaultPort);
            }
            try {
                return new Endpoint(
                        trimmed.substring(0, colon), Integer.parseInt(trimmed.substring(colon + 1)));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid entry in hosts: '" + address + "', expected host:port", e);
            }
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }
}
