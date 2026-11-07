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

package org.apache.skywalking.oap.server.core.alarm;

import io.grpc.Status;
import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-local gRPC alarm hook delivery metrics ({@code telemetry.prometheus}).
 * Labels are message kind / result only.
 */
public final class AlarmGrpcDeliveryMetrics {
    public static final String KIND_ALARM = "alarm";
    public static final String KIND_RECOVERY = "recovery";

    public static final String RESULT_SUCCESS = "success";
    public static final String RESULT_TIMEOUT = "timeout";
    public static final String RESULT_UNAVAILABLE = "unavailable";
    public static final String RESULT_ERROR = "error";
    public static final String RESULT_INTERRUPTED = "interrupted";

    private final MetricsCreator metricsCreator;
    private final ConcurrentMap<String, CounterMetrics> deliveryTotals = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CounterMetrics> messageTotals = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, HistogramMetrics> latencies = new ConcurrentHashMap<>();

    public AlarmGrpcDeliveryMetrics(final MetricsCreator metricsCreator) {
        this.metricsCreator = Objects.requireNonNull(metricsCreator, "metricsCreator");
    }

    /**
     * Record one finished gRPC delivery attempt. Never throws.
     *
     * @param messageKind {@link #KIND_ALARM} or {@link #KIND_RECOVERY}
     * @param messageCount number of alarm / recovery messages in this stream
     * @param seconds wall time in seconds
     */
    public void record(final String messageKind,
                       final String result,
                       final int messageCount,
                       final double seconds) {
        try {
            final String kind = Objects.requireNonNull(messageKind, "messageKind");
            final String outcome = Objects.requireNonNull(result, "result");
            deliveryTotals.computeIfAbsent(kind + '\0' + outcome, ignored -> metricsCreator.createCounter(
                "alarm_grpc_delivery_total",
                "gRPC alarm hook delivery attempts by message kind and result",
                new MetricsTag.Keys("message_kind", "result"),
                new MetricsTag.Values(kind, outcome)
            )).inc();

            messageTotals.computeIfAbsent(kind + '\0' + outcome, ignored -> metricsCreator.createCounter(
                "alarm_grpc_delivery_messages_total",
                "gRPC alarm hook messages sent by message kind and result",
                new MetricsTag.Keys("message_kind", "result"),
                new MetricsTag.Values(kind, outcome)
            )).inc(Math.max(0, messageCount));

            latencies.computeIfAbsent(kind, ignored -> metricsCreator.createHistogramMetric(
                "alarm_grpc_delivery_latency_seconds",
                "gRPC alarm hook delivery latency in seconds",
                new MetricsTag.Keys("message_kind"),
                new MetricsTag.Values(kind),
                .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30
            )).observe(Math.max(0, seconds));
        } catch (Throwable ignored) {
            // so11y must not break alarm delivery
        }
    }

    /**
     * Classify a failed stream by its gRPC status when present.
     * Uses {@link Status#fromThrowable(Throwable)} so {@code StatusException},
     * {@code StatusRuntimeException} and nested wraps all resolve the same way.
     */
    public static String resultOf(final Throwable error) {
        if (error == null) {
            return RESULT_ERROR;
        }
        switch (Status.fromThrowable(error).getCode()) {
            case DEADLINE_EXCEEDED:
                return RESULT_TIMEOUT;
            case UNAVAILABLE:
                return RESULT_UNAVAILABLE;
            case CANCELLED:
                return RESULT_INTERRUPTED;
            default:
                return RESULT_ERROR;
        }
    }
}
