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

    /** Open producer batches are flushed this often, whatever their size. */
    public long producerLingerMs = 1;

    /**
     * Batches a producer may have in flight before {@code sendAsync} blocks the worker's load thread,
     * which is the driver's backpressure. 0 disables the cap.
     */
    public int producerMaxInFlightBatches = 16;

    /** Maximum number of messages one consumer poll asks for. */
    public int consumerPollSize = 1000;
}
