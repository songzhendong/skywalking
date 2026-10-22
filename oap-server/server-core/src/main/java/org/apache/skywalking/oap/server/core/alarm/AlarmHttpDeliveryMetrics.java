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

import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-local HTTP alarm hook delivery metrics ({@code telemetry.prometheus}).
 * Labels are hook type / result / status class only.
 */
public final class AlarmHttpDeliveryMetrics {
    public static final String RESULT_SUCCESS = "success";
    public static final String RESULT_HTTP_ERROR = "http_error";
    public static final String RESULT_TIMEOUT = "timeout";
    public static final String RESULT_IO_ERROR = "io_error";
    public static final String RESULT_INTERRUPTED = "interrupted";

    public static final String STATUS_2XX = "2xx";
    public static final String STATUS_3XX = "3xx";
    public static final String STATUS_4XX = "4xx";
    public static final String STATUS_5XX = "5xx";
    public static final String STATUS_OTHER = "other";

    /** No-op when telemetry is unavailable. */
    public static final AlarmHttpDeliveryMetrics NOOP = new AlarmHttpDeliveryMetrics();

    private final MetricsCreator metricsCreator;
    private final ConcurrentMap<String, CounterMetrics> deliveryTotals = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CounterMetrics> statusTotals = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, HistogramMetrics> latencies = new ConcurrentHashMap<>();

    private AlarmHttpDeliveryMetrics() {
        this.metricsCreator = null;
    }

    public AlarmHttpDeliveryMetrics(final MetricsCreator metricsCreator) {
        this.metricsCreator = metricsCreator;
    }

    /**
     * Record one finished delivery attempt. Never throws.
     *
     * @param statusClass {@code null} when there is no HTTP status (timeout / IO / interrupt)
     * @param seconds     wall time in seconds
     */
    public void record(final String hookType,
                       final String result,
                       final String statusClass,
                       final double seconds) {
        if (metricsCreator == null) {
            return;
        }
        try {
            final String type = sanitize(hookType);
            final String outcome = sanitize(result);
            deliveryTotals.computeIfAbsent(type + '\0' + outcome, ignored -> metricsCreator.createCounter(
                "alarm_http_delivery_total",
                "HTTP alarm hook delivery attempts by hook type and result",
                new MetricsTag.Keys("hook_type", "result"),
                new MetricsTag.Values(type, outcome)
            )).inc();

            if (statusClass != null) {
                final String clazz = sanitize(statusClass);
                statusTotals.computeIfAbsent(type + '\0' + clazz, ignored -> metricsCreator.createCounter(
                    "alarm_http_delivery_status_total",
                    "HTTP alarm hook responses by hook type and status class",
                    new MetricsTag.Keys("hook_type", "status_class"),
                    new MetricsTag.Values(type, clazz)
                )).inc();
            }

            latencies.computeIfAbsent(type, ignored -> metricsCreator.createHistogramMetric(
                "alarm_http_delivery_latency_seconds",
                "HTTP alarm hook delivery latency in seconds",
                new MetricsTag.Keys("hook_type"),
                new MetricsTag.Values(type),
                .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30
            )).observe(Math.max(0, seconds));
        } catch (Throwable ignored) {
            // so11y must not break alarm delivery
        }
    }

    public static String statusClassOf(final int status) {
        if (status >= 200 && status < 300) {
            return STATUS_2XX;
        }
        if (status >= 300 && status < 400) {
            return STATUS_3XX;
        }
        if (status >= 400 && status < 500) {
            return STATUS_4XX;
        }
        if (status >= 500 && status < 600) {
            return STATUS_5XX;
        }
        return STATUS_OTHER;
    }

    private static String sanitize(final String value) {
        if (value == null || value.isEmpty()) {
            return "unknown";
        }
        return value;
    }
}
