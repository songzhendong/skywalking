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

import io.grpc.Status;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.GaugeMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;

/**
 * Client-side cluster remote RPC metrics ({@code telemetry.prometheus}):
 * {@code remote_out_*} count/latency/error, plus peer stream pressure gauges.
 */
public final class RemoteClientMetrics {
    public static final String REASON_ERROR = "error";
    public static final String REASON_TIMEOUT = "timeout";
    public static final String REASON_IO_ERROR = "io_error";
    public static final String REASON_RATE_LIMITED = "rate_limited";
    public static final String REASON_AUTH_FAILED = "auth_failed";
    public static final String REASON_SERVER_ERROR = "server_error";
    public static final String REASON_REJECTED = "rejected";
    public static final String REASON_INTERRUPTED = "interrupted";

    private final MetricsCreator metricsCreator;
    private final String dest;
    private final String self;

    private final CounterMetrics outCounter;
    private final HistogramMetrics outLatency;
    private final GaugeMetrics streamInflight;
    private final CounterMetrics streamBlockSeconds;
    private final GaugeMetrics queueUsedPercentage;
    private final ConcurrentMap<String, CounterMetrics> errorCounters = new ConcurrentHashMap<>();

    public static RemoteClientMetrics forGrpc(final MetricsCreator metricsCreator, final Address address) {
        return new RemoteClientMetrics(metricsCreator, address, false, true);
    }

    public static RemoteClientMetrics forSelf(final MetricsCreator metricsCreator, final Address address) {
        return new RemoteClientMetrics(metricsCreator, address, true, false);
    }

    private RemoteClientMetrics(final MetricsCreator metricsCreator,
                                final Address address,
                                final boolean self,
                                final boolean trackStreamAndQueue) {
        this.metricsCreator = Objects.requireNonNull(metricsCreator, "metricsCreator");
        this.dest = Objects.requireNonNull(address, "address").toString();
        this.self = self ? "Y" : "N";

        this.outCounter = metricsCreator.createCounter(
            "remote_out_count",
            "The number(client side) of inside remote inside aggregate rpc.",
            new MetricsTag.Keys("dest", "self"),
            new MetricsTag.Values(this.dest, this.self)
        );
        this.outLatency = metricsCreator.createHistogramMetric(
            "remote_out_latency",
            "The latency(client side) of inside remote inside aggregate rpc.",
            new MetricsTag.Keys("dest", "self"),
            new MetricsTag.Values(this.dest, this.self)
        );
        if (trackStreamAndQueue) {
            this.streamInflight = metricsCreator.createGauge(
                "remote_out_stream_inflight",
                "In-flight gRPC streams for one remote client.",
                new MetricsTag.Keys("dest", "self"),
                new MetricsTag.Values(this.dest, this.self)
            );
            this.streamBlockSeconds = metricsCreator.createCounter(
                "remote_out_stream_block_seconds",
                "Seconds spent waiting for a free remote gRPC stream slot.",
                new MetricsTag.Keys("dest", "self"),
                new MetricsTag.Values(this.dest, this.self)
            );
            // Seed 0 so Prometheus exports the labeled series before any slot wait.
            this.streamBlockSeconds.inc(0);
            this.queueUsedPercentage = metricsCreator.createGauge(
                "remote_out_queue_used_percentage",
                "Send queue used percentage for one remote client.",
                new MetricsTag.Keys("dest", "self"),
                new MetricsTag.Values(this.dest, this.self)
            );
        } else {
            this.streamInflight = null;
            this.streamBlockSeconds = null;
            this.queueUsedPercentage = null;
        }
    }

    public void incOut() {
        outCounter.inc();
    }

    public void observeLatencySeconds(final double seconds) {
        outLatency.observe(Math.max(0, seconds));
    }

    public void recordError(final String reason) {
        final String outcome = reason == null || reason.isEmpty() ? REASON_ERROR : reason;
        errorCounters.computeIfAbsent(outcome, ignored -> metricsCreator.createCounter(
            "remote_out_error_count",
            "The error number(client side) of inside remote inside aggregate rpc.",
            new MetricsTag.Keys("dest", "self", "reason"),
            new MetricsTag.Values(dest, self, outcome)
        )).inc();
    }

    public void setStreamInflight(final int value) {
        if (streamInflight != null) {
            streamInflight.setValue(Math.max(0, value));
        }
    }

    public void addStreamBlockSeconds(final double seconds) {
        if (streamBlockSeconds != null && seconds > 0) {
            streamBlockSeconds.inc(seconds);
        }
    }

    public void setQueueUsedPercentage(final double percentage) {
        if (queueUsedPercentage != null) {
            queueUsedPercentage.setValue(Math.max(0, Math.min(100, percentage)));
        }
    }

    /** Zero peer pressure gauges on close so a departed peer leaves no stale sample. */
    public void clearPressureGauges() {
        setStreamInflight(0);
        setQueueUsedPercentage(0);
    }

    /** Map a finished gRPC status to a stable {@code reason} label. */
    public static String reasonOf(final Status status) {
        if (status == null) {
            return REASON_ERROR;
        }
        switch (status.getCode()) {
            case OK:
                return REASON_ERROR;
            case DEADLINE_EXCEEDED:
                return REASON_TIMEOUT;
            case UNAVAILABLE:
                return REASON_IO_ERROR;
            case RESOURCE_EXHAUSTED:
                return REASON_RATE_LIMITED;
            case UNAUTHENTICATED:
            case PERMISSION_DENIED:
                return REASON_AUTH_FAILED;
            case UNKNOWN:
            case INTERNAL:
            case DATA_LOSS:
            case UNIMPLEMENTED:
                return REASON_SERVER_ERROR;
            case CANCELLED:
                return REASON_REJECTED;
            default:
                return REASON_REJECTED;
        }
    }
}
