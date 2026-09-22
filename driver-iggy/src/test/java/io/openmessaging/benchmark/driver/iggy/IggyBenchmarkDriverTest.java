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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

@EnabledIfEnvironmentVariable(named = "IGGY_DRIVER_TEST_PORT", matches = "[0-9]+")
class IggyBenchmarkDriverTest {
    @TempDir Path directory;

    @Test
    @Timeout(60)
    void concurrentProducersAndConsumerDeliverAndCommitEveryMessage() throws Exception {
        int port = Integer.parseInt(System.getenv("IGGY_DRIVER_TEST_PORT"));
        String stream = "driver-validation-" + UUID.randomUUID();
        String topic = "messages";
        String group = "readers";
        Path configuration = directory.resolve("driver.yaml");
        Files.writeString(
                configuration,
                "host: 127.0.0.1\nport: "
                        + port
                        + "\nstreamName: "
                        + stream
                        + "\nioThreads: 2\nproducerFlushThreads: 2"
                        + "\nproducerBatchSize: 8\nproducerLingerMs: 1\nproducerMaxInFlightBatches: 5"
                        + "\nconsumerAutoCommit: false\nconsumerPollConcurrency: 4\nconsumerCommitIntervalMs: 10\n");
        int count = 256;
        CountDownLatch received = new CountDownLatch(count);
        Set<Integer> unique = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        ConsumerCallback callback =
                new ConsumerCallback() {
                    @Override
                    public void messageReceived(byte[] payload, long timestamp) {
                        messageReceived(ByteBuffer.wrap(payload), timestamp);
                    }

                    @Override
                    public void messageReceived(ByteBuffer payload, long timestamp) {
                        if (!unique.add(payload.getInt())) {
                            duplicates.incrementAndGet();
                        }
                        received.countDown();
                    }
                };
        try (IggyBenchmarkDriver driver = new IggyBenchmarkDriver()) {
            driver.initialize(configuration.toFile(), mock(StatsLogger.class));
            driver.createTopic(topic, 4).get(10, TimeUnit.SECONDS);
            var consumer = driver.createConsumer(topic, group, callback).get(10, TimeUnit.SECONDS);
            var first = driver.createProducer(topic).get(10, TimeUnit.SECONDS);
            var second = driver.createProducer(topic).get(10, TimeUnit.SECONDS);
            List<CompletableFuture<Void>> sends = new ArrayList<>();
            for (int index = 0; index < count; index++) {
                var producer = index % 2 == 0 ? first : second;
                sends.add(
                        producer.sendAsync(Optional.empty(), ByteBuffer.allocate(4).putInt(index).array()));
            }
            CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).get(15, TimeUnit.SECONDS);
            assertTrue(received.await(15, TimeUnit.SECONDS));
            consumer.close();
            assertEquals(count, unique.size());
            assertEquals(0, duplicates.get());
            var probe = AsyncIggyTcpClient.builder().host("127.0.0.1").port(port).build();
            try {
                probe
                        .connect()
                        .thenCompose(ignored -> probe.users().login("iggy", "iggy"))
                        .get(5, TimeUnit.SECONDS);
                long committed = 0;
                for (long partition = 0; partition < 4; partition++) {
                    var offset =
                            probe
                                    .consumerOffsets()
                                    .getConsumerOffset(
                                            StreamId.of(stream),
                                            TopicId.of(topic),
                                            Optional.of(partition),
                                            Consumer.group(ConsumerId.of(group)))
                                    .get(5, TimeUnit.SECONDS);
                    assertTrue(offset.isPresent());
                    committed += offset.orElseThrow().storedOffset().longValueExact() + 1;
                }
                assertEquals(count, committed);
            } finally {
                probe.close().get(5, TimeUnit.SECONDS);
            }
        }
    }
}
