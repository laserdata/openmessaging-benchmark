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


import java.time.Duration;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;

/**
 * Settles a replicated mutation whose outcome the SDK could not establish.
 *
 * <p>A send that went out without a proved reply ends with {@code OPERATION_UNCERTAIN} and its
 * operation id. The SDK keeps that operation and blocks its lane, one partition of one connection:
 * every later mutation of the lane ends at once with {@code LANE_BLOCKED} and the same id, and no
 * request goes out. The lane opens again only when the caller resolves the operation, which replays
 * the kept request with its original id so that the server cannot apply it twice, or abandons it.
 * The SDK does neither on its own, so without this class one lost reply blocks a partition until
 * the connection closes.
 */
final class UncertainOperations {

    /** What the driver does with an operation that blocks a lane. */
    enum Policy {
        /**
         * Replay the operation until the server proves its outcome, like the retries of Kafka's
         * idempotent producer.
         */
        RESOLVE,
        /** Drop the operation at once. Its request counts as failed, though it can have committed. */
        ABANDON;

        static Policy parse(String value) {
            String policy = value == null ? "resolve" : value;
            switch (policy.toLowerCase(Locale.ROOT)) {
                case "resolve":
                    return RESOLVE;
                case "abandon":
                    return ABANDON;
                default:
                    throw new IllegalArgumentException(
                            "Unknown uncertainOperationPolicy '" + value + "', expected resolve or abandon");
            }
        }
    }

    private UncertainOperations() {}

    /**
     * The operation that blocks the lane of a failed mutation.
     *
     * @param error the failure, possibly wrapped in completion or execution exceptions
     * @return the operation id of an {@code OPERATION_UNCERTAIN} or {@code LANE_BLOCKED} failure,
     *     else empty
     */
    static OptionalLong blockingOperation(Throwable error) {
        if (IggyBenchmarkDriver.unwrap(error) instanceof IggyOperationException operation
                && (operation.getErrorCode() == IggyErrorCode.OPERATION_UNCERTAIN
                        || operation.getErrorCode() == IggyErrorCode.LANE_BLOCKED)) {
            return OptionalLong.of(operation.getOperationId());
        }
        return OptionalLong.empty();
    }

    /**
     * Whether the failed request went out and can have committed. A {@code LANE_BLOCKED} request
     * never went out.
     *
     * @param error the failure, possibly wrapped in completion or execution exceptions
     * @return true for {@code OPERATION_UNCERTAIN}
     */
    static boolean isUncertain(Throwable error) {
        return IggyBenchmarkDriver.unwrap(error) instanceof IggyOperationException operation
                && operation.getErrorCode() == IggyErrorCode.OPERATION_UNCERTAIN;
    }

    /**
     * Settles the operation, so that its lane opens again.
     *
     * <p>With {@link Policy#RESOLVE} every attempt replays the operation for up to {@code timeout}.
     * An attempt that ends uncertain again runs again through {@code retryLater}, until {@code stop}
     * says to give up. The operation is then abandoned. With {@link Policy#ABANDON} the operation is
     * abandoned at once.
     *
     * <p>The returned future completes on an SDK event loop or on a {@code retryLater} thread, so
     * its callbacks must not block.
     *
     * @param client the connection whose session keeps the operation
     * @param operationId the id from the {@code OPERATION_UNCERTAIN} or {@code LANE_BLOCKED} failure
     * @param policy resolve or abandon
     * @param timeout the time one replay may take
     * @param retryLater runs the next replay after a pause
     * @param stop checked after each uncertain replay; true abandons the operation
     * @return a future that completes normally when a replay proved that the operation committed. It
     *     fails with the proved rejection, or with {@code OPERATION_UNCERTAIN} when the outcome
     *     stays unknown. In every case the lane no longer waits for the operation.
     */
    static CompletableFuture<Void> settle(
            AsyncIggyTcpClient client,
            long operationId,
            Policy policy,
            Duration timeout,
            Executor retryLater,
            BooleanSupplier stop) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (policy == Policy.ABANDON) {
            abandon(
                    client,
                    operationId,
                    new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, operationId),
                    result);
        } else {
            resolve(client, operationId, timeout, retryLater, stop, result);
        }
        return result;
    }

    private static void resolve(
            AsyncIggyTcpClient client,
            long operationId,
            Duration timeout,
            Executor retryLater,
            BooleanSupplier stop,
            CompletableFuture<Void> result) {
        call(() -> client.resolveOperation(operationId, timeout))
                .whenComplete(
                        (body, error) -> {
                            if (error == null) {
                                result.complete(null);
                                return;
                            }
                            Throwable cause = IggyBenchmarkDriver.unwrap(error);
                            if (isUncertain(cause) && !stop.getAsBoolean()) {
                                try {
                                    retryLater.execute(
                                            () -> resolve(client, operationId, timeout, retryLater, stop, result));
                                    return;
                                } catch (RejectedExecutionException stopped) {
                                    // The driver is closing, so the operation is abandoned below.
                                }
                            }
                            // A proved outcome or a lost one already freed the lane. Abandon
                            // frees it in every other case.
                            abandon(client, operationId, cause, result);
                        });
    }

    /**
     * Drops what the client keeps of the operation, then fails the result with the outcome.
     *
     * @param client the connection whose session keeps the operation
     * @param operationId the operation to drop
     * @param outcome the failure the result ends with
     * @param result the future of {@link #settle}
     */
    private static void abandon(
            AsyncIggyTcpClient client,
            long operationId,
            Throwable outcome,
            CompletableFuture<Void> result) {
        // OPERATION_NOT_RETAINED only says that nothing is kept any more, so every failure of the
        // abandon itself is ignored.
        call(() -> client.abandonOperation(operationId))
                .whenComplete((ignored, error) -> result.completeExceptionally(outcome));
    }

    private static <T> CompletableFuture<T> call(Supplier<CompletableFuture<T>> request) {
        try {
            return request.get();
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
    }
}
