/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.core.remote.client;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.remote.data.StreamData;
import org.apache.skywalking.oap.server.core.remote.grpc.proto.Empty;
import org.apache.skywalking.oap.server.core.remote.grpc.proto.RemoteMessage;
import org.apache.skywalking.oap.server.core.remote.grpc.proto.RemoteServiceGrpc;
import org.apache.skywalking.oap.server.library.batchqueue.BatchQueue;
import org.apache.skywalking.oap.server.library.batchqueue.BatchQueueConfig;
import org.apache.skywalking.oap.server.library.batchqueue.BatchQueueManager;
import org.apache.skywalking.oap.server.library.batchqueue.BufferStrategy;
import org.apache.skywalking.oap.server.library.batchqueue.HandlerConsumer;
import org.apache.skywalking.oap.server.library.batchqueue.ThreadPolicy;
import org.apache.skywalking.oap.server.library.client.grpc.GRPCClient;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;

/**
 * This is a wrapper of the gRPC client for sending message to each other OAP server. It contains a block queue to
 * buffering the message and sending the message by batch.
 */
@Slf4j
public class GRPCRemoteClient implements RemoteClient {
    /** Max concurrent outbound gRPC streams for one peer. */
    static final int MAX_CONCURRENT_STREAMS = 10;

    private final int bufferSize;
    private final Address address;
    private final AtomicInteger concurrentStreamObserverNumber = new AtomicInteger(0);
    /** Guards inflight counter updates against concurrent gRPC callbacks. */
    private final Object inflightLock = new Object();
    private SslContext sslContext;
    private GRPCClient client;
    private BatchQueue<RemoteMessage> queue;
    private String queueName;
    private boolean isConnect;
    private volatile boolean closed;
    private final RemoteClientMetrics metrics;
    private int remoteTimeout;
    private long lastRemoteResourceExhaustedTime = 0;

    public GRPCRemoteClient(final ModuleDefineHolder moduleDefineHolder,
                            final Address address,
                            final int bufferSize,
                            final int remoteTimeout,
                            final SslContext sslContext) {

        this.address = address;
        this.bufferSize = bufferSize;
        this.remoteTimeout = remoteTimeout;
        this.sslContext = sslContext;

        final MetricsCreator metricsCreator = moduleDefineHolder.find(TelemetryModule.NAME)
                                                                .provider()
                                                                .getService(MetricsCreator.class);
        this.metrics = RemoteClientMetrics.forGrpc(metricsCreator, address);
    }

    @Override
    public void connect() {
        if (!isConnect) {
            this.getClient().connect();
            this.queueName = "GRPC_REMOTE_" + address.getHost() + "_" + address.getPort();
            final BatchQueueConfig<RemoteMessage> config = BatchQueueConfig.<RemoteMessage>builder()
                .threads(ThreadPolicy.fixed(1))
                .bufferSize(bufferSize)
                .strategy(BufferStrategy.BLOCKING)
                .consumer(new RemoteMessageConsumer())
                .minIdleMs(10)
                .maxIdleMs(100)
                .build();

            this.queue = BatchQueueManager.create(queueName, config);
            this.isConnect = true;
        }
    }

    /**
     * Get channel state by the true value of request connection.
     *
     * @return a channel when the state to be ready
     */
    @Override
    public ManagedChannel getChannel() {
        return getClient().getChannel();
    }

    GRPCClient getClient() {
        if (Objects.isNull(client)) {
            synchronized (GRPCRemoteClient.class) {
                if (Objects.isNull(client)) {
                    this.client = new GRPCClient(address.getHost(), address.getPort(), sslContext);
                }
            }
        }
        return client;
    }

    RemoteServiceGrpc.RemoteServiceStub getStub() {
        return RemoteServiceGrpc.newStub(getChannel());
    }

    /**
     * Push stream data which need to send to another OAP server.
     *
     * @param nextWorkerName the name of a worker which will process this stream data.
     * @param streamData     the entity contains the values.
     */
    @Override
    public void push(String nextWorkerName, StreamData streamData) {
        RemoteMessage.Builder builder = RemoteMessage.newBuilder();
        builder.setNextWorkerName(nextWorkerName);
        builder.setRemoteData(streamData.serialize());

        // Closed clients are torn down; skip so peer lifecycle does not inflate remote_out_error.
        if (closed) {
            return;
        }
        // Publish queue usage before produce so BLOCKING backpressure is visible.
        publishQueueUsed();
        if (!this.queue.produce(builder.build())) {
            // produce() can still fail after close races the shutdown path above.
            if (!closed) {
                metrics.recordError(RemoteClientMetrics.REASON_ERROR);
            }
            return;
        }
        publishQueueUsed();
    }

    class RemoteMessageConsumer implements HandlerConsumer<RemoteMessage> {
        @Override
        public void consume(List<RemoteMessage> remoteMessages) {
            publishQueueUsed();
            final int messageCount = remoteMessages.size();
            final long[] startNanos = new long[1];
            final AtomicBoolean streamAcquired = new AtomicBoolean(false);
            final AtomicBoolean streamFinished = new AtomicBoolean(false);
            final AtomicBoolean errorRecorded = new AtomicBoolean(false);
            try {
                StreamObserver<RemoteMessage> streamObserver = createStreamObserver(
                    startNanos, messageCount, streamAcquired, streamFinished, errorRecorded);
                publishQueueUsed();
                for (RemoteMessage remoteMessage : remoteMessages) {
                    metrics.incOut();
                    streamObserver.onNext(remoteMessage);
                }
                streamObserver.onCompleted();
            } catch (RemoteClientClosedException t) {
                // close() unblocked a stream wait or raced acquire — not an outbound delivery error.
            } catch (RemoteStreamInterruptedException t) {
                // reason=interrupted already recorded while waiting for a stream slot.
                log.error(t.getMessage(), t);
            } catch (Throwable t) {
                if (streamAcquired.get()) {
                    endStream(streamFinished, startNanos[0], messageCount, false);
                }
                // Prefer gRPC Status when present; skip if onError already recorded or client closed.
                if (!closed && errorRecorded.compareAndSet(false, true)) {
                    if (t instanceof StatusRuntimeException || t instanceof StatusException) {
                        metrics.recordError(RemoteClientMetrics.reasonOf(Status.fromThrowable(t)));
                    } else {
                        metrics.recordError(RemoteClientMetrics.REASON_ERROR);
                    }
                }
                log.error(t.getMessage(), t);
            } finally {
                publishQueueUsed();
            }
        }
    }

    /**
     * Create a gRPC stream observer for one consume batch. Concurrency is capped at
     * {@link #MAX_CONCURRENT_STREAMS}.
     */
    private StreamObserver<RemoteMessage> createStreamObserver(final long[] startNanosHolder,
                                                               final int messageCount,
                                                               final AtomicBoolean streamAcquired,
                                                               final AtomicBoolean streamFinished,
                                                               final AtomicBoolean errorRecorded) {
        int sleepTotalMillis = 0;
        int sleepMillis = 10;
        long blockedNanos = 0;

        // Wait while the stream limit is already reached; increment happens after this loop.
        while (concurrentStreamObserverNumber.get() >= MAX_CONCURRENT_STREAMS) {
            if (closed) {
                throw new RemoteClientClosedException("Remote client closed while waiting for a stream slot");
            }
            final long blockStart = System.nanoTime();
            publishQueueUsed();
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                blockedNanos += System.nanoTime() - blockStart;
                metrics.addStreamBlockSeconds(blockedNanos / 1_000_000_000d);
                metrics.recordError(RemoteClientMetrics.REASON_INTERRUPTED);
                throw new RemoteStreamInterruptedException("Interrupted while waiting for a remote stream slot", e);
            }
            blockedNanos += System.nanoTime() - blockStart;

            sleepTotalMillis += sleepMillis;

            if (sleepTotalMillis > 60000) {
                log.warn(
                    "Remote client [{}] block times over 60 seconds. Current streaming number {}",
                    address, concurrentStreamObserverNumber.get()
                );
                sleepTotalMillis = 0;
            }
        }
        metrics.addStreamBlockSeconds(blockedNanos / 1_000_000_000d);
        startNanosHolder[0] = System.nanoTime();

        // Acquire before call() so a fast completion cannot race ahead of the increment.
        // close() may have zeroed the counter and unblocked the wait — refuse to re-inflate gauges.
        if (!acquireStreamSlot()) {
            throw new RemoteClientClosedException("Remote client closed before acquiring a stream slot");
        }
        streamAcquired.set(true);
        try {
            return getStub().withDeadlineAfter(remoteTimeout, TimeUnit.SECONDS)
                            .call(new StreamObserver<Empty>() {
                                @Override
                                public void onNext(Empty empty) {
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                    endStream(streamFinished, startNanosHolder[0], messageCount, false);
                                    Status status = Status.fromThrowable(throwable);
                                    if (!closed && errorRecorded.compareAndSet(false, true)) {
                                        metrics.recordError(RemoteClientMetrics.reasonOf(status));
                                    }
                                    if (closed) {
                                        return;
                                    }
                                    if (Status.CANCELLED.getCode() == status.getCode()) {
                                        if (log.isDebugEnabled()) {
                                            log.debug(throwable.getMessage(), throwable);
                                        }
                                        return;
                                    } else if (Status.RESOURCE_EXHAUSTED.getCode() == status.getCode()) {
                                        if (System.currentTimeMillis() - lastRemoteResourceExhaustedTime > 120_000) {
                                            log.warn(throwable.getMessage(), throwable);
                                            lastRemoteResourceExhaustedTime = System.currentTimeMillis();
                                        }
                                        return;
                                    }
                                    log.error(throwable.getMessage(), throwable);
                                }

                                @Override
                                public void onCompleted() {
                                    endStream(streamFinished, startNanosHolder[0], messageCount, true);
                                }
                            });
        } catch (RuntimeException | Error t) {
            endStream(streamFinished, startNanosHolder[0], messageCount, false);
            throw t;
        }
    }

    /**
     * @return {@code false} when the client is already closed (do not bump inflight gauges)
     */
    private boolean acquireStreamSlot() {
        synchronized (inflightLock) {
            if (closed) {
                return false;
            }
            final int inflight = concurrentStreamObserverNumber.incrementAndGet();
            metrics.setStreamInflight(inflight);
            return true;
        }
    }

    private void publishQueueUsed() {
        if (queue != null) {
            metrics.setQueueUsedPercentage(queue.stats().totalUsedPercentage());
        }
    }

    /** Release one inflight slot at most once; record latency only for successful streams. */
    private void endStream(final AtomicBoolean streamFinished,
                           final long startNanos,
                           final int messageCount,
                           final boolean observeLatency) {
        if (!streamFinished.compareAndSet(false, true)) {
            return;
        }
        synchronized (inflightLock) {
            if (!closed) {
                final int inflight = concurrentStreamObserverNumber.addAndGet(-1);
                metrics.setStreamInflight(Math.max(0, inflight));
            }
        }
        if (closed || !observeLatency) {
            return;
        }
        // One observation per successful stream (batch RTT), not per message.
        metrics.observeLatencySeconds((System.nanoTime() - startNanos) / 1_000_000_000d);
    }

    @Override
    public void close() {
        // Drain while still open; closed-first would drop the final flush silently.
        if (queueName != null) {
            BatchQueueManager.shutdown(queueName);
        }
        // Ignore late gRPC callbacks after close.
        synchronized (inflightLock) {
            closed = true;
            concurrentStreamObserverNumber.set(0);
            metrics.clearPressureGauges();
        }
        if (Objects.nonNull(this.client)) {
            this.client.shutdown();
        }
    }

    @Override
    public Address getAddress() {
        return address;
    }

    @Override
    public int compareTo(RemoteClient o) {
        return address.compareTo(o.getAddress());
    }

    /** Stream-slot wait was interrupted; {@code reason=interrupted} already recorded. */
    static final class RemoteStreamInterruptedException extends RuntimeException {
        RemoteStreamInterruptedException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /** Client closed during stream wait/acquire; do not count as remote_out_error. */
    static final class RemoteClientClosedException extends RuntimeException {
        RemoteClientClosedException(final String message) {
            super(message);
        }
    }
}
