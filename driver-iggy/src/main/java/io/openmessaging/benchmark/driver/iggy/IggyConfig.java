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

/** Driver settings read from the driver yaml (see {@code iggy.yaml}). */
public class IggyConfig {

    /** Host of the Iggy TCP listener. */
    public String host = "127.0.0.1";

    /** Port of the Iggy TCP listener. */
    public int port = 8090;

    /** User every connection logs in with. */
    public String username = "iggy";

    /** Password every connection logs in with. */
    public String password = "iggy";

    /** Stream that holds the benchmark topics. Created when missing. */
    public String streamName = "omb";

    /** A producer batch is flushed once it holds this many messages. */
    public int producerBatchSize = 1000;

    /** A producer batch is flushed once its payloads reach this many bytes. */
    public long producerBatchBytes = 1024 * 1024;

    /** Open producer batches are flushed this often, whatever their size. */
    public long producerLingerMs = 1;

    /** Maximum number of messages one consumer poll asks for. */
    public int consumerPollSize = 1000;
}
