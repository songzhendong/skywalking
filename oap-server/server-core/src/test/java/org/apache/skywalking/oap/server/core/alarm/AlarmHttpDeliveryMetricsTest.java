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
import org.apache.skywalking.oap.server.telemetry.api.GaugeMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AlarmHttpDeliveryMetricsTest {

    @Test
    public void statusClassOfMapsHttpRanges() {
        assertEquals(AlarmHttpDeliveryMetrics.STATUS_2XX, AlarmHttpDeliveryMetrics.statusClassOf(204));
        assertEquals(AlarmHttpDeliveryMetrics.STATUS_3XX, AlarmHttpDeliveryMetrics.statusClassOf(302));
        assertEquals(AlarmHttpDeliveryMetrics.STATUS_4XX, AlarmHttpDeliveryMetrics.statusClassOf(429));
        assertEquals(AlarmHttpDeliveryMetrics.STATUS_5XX, AlarmHttpDeliveryMetrics.statusClassOf(500));
        assertEquals(AlarmHttpDeliveryMetrics.STATUS_OTHER, AlarmHttpDeliveryMetrics.statusClassOf(100));
    }

    @Test
    public void recordIncrementsCountersAndObservesLatency() {
        final RecordingCreator creator = new RecordingCreator();
        final AlarmHttpDeliveryMetrics metrics = new AlarmHttpDeliveryMetrics(creator);

        metrics.record("webhook", AlarmHttpDeliveryMetrics.RESULT_SUCCESS, AlarmHttpDeliveryMetrics.STATUS_2XX, 0.12);
        metrics.record("webhook", AlarmHttpDeliveryMetrics.RESULT_HTTP_ERROR, AlarmHttpDeliveryMetrics.STATUS_5XX, 0.2);
        metrics.record("slack", AlarmHttpDeliveryMetrics.RESULT_TIMEOUT, null, 1.0);

        assertEquals(3, creator.deliveryIncrements.get());
        assertEquals(2, creator.statusIncrements.get());
        assertEquals(3, creator.latencyObservations.size());
        assertTrue(creator.latencyObservations.stream().allMatch(v -> v >= 0));
    }

    @Test
    public void recordSwallowsCreatorFailures() {
        final MetricsCreator broken = new MetricsCreator() {
            @Override
            public CounterMetrics createCounter(String name, String tips, MetricsTag.Keys tagKeys,
                                                MetricsTag.Values tagValues) {
                throw new IllegalStateException("boom");
            }

            @Override
            public GaugeMetrics createGauge(String name, String tips, MetricsTag.Keys tagKeys,
                                            MetricsTag.Values tagValues) {
                throw new IllegalStateException("boom");
            }

            @Override
            public HistogramMetrics createHistogramMetric(String name, String tips, MetricsTag.Keys tagKeys,
                                                          MetricsTag.Values tagValues, double... buckets) {
                throw new IllegalStateException("boom");
            }
        };

        new AlarmHttpDeliveryMetrics(broken)
            .record("webhook", AlarmHttpDeliveryMetrics.RESULT_SUCCESS, AlarmHttpDeliveryMetrics.STATUS_2XX, 0.01);
    }

    @Test
    public void noopDoesNothing() {
        AlarmHttpDeliveryMetrics.NOOP.record(
            "webhook", AlarmHttpDeliveryMetrics.RESULT_SUCCESS, AlarmHttpDeliveryMetrics.STATUS_2XX, 0.01);
    }

    private static final class RecordingCreator implements MetricsCreator {
        private final AtomicInteger deliveryIncrements = new AtomicInteger();
        private final AtomicInteger statusIncrements = new AtomicInteger();
        private final List<Double> latencyObservations = new ArrayList<>();

        @Override
        public CounterMetrics createCounter(String name, String tips, MetricsTag.Keys tagKeys,
                                            MetricsTag.Values tagValues) {
            return new CounterMetrics() {
                @Override
                public void inc() {
                    if ("alarm_http_delivery_total".equals(name)) {
                        deliveryIncrements.incrementAndGet();
                    } else if ("alarm_http_delivery_status_total".equals(name)) {
                        statusIncrements.incrementAndGet();
                    }
                }

                @Override
                public void inc(double value) {
                    inc();
                }
            };
        }

        @Override
        public GaugeMetrics createGauge(String name, String tips, MetricsTag.Keys tagKeys,
                                        MetricsTag.Values tagValues) {
            return new GaugeMetrics() {
                @Override
                public void inc() {
                }

                @Override
                public void inc(double value) {
                }

                @Override
                public void dec() {
                }

                @Override
                public void dec(double value) {
                }

                @Override
                public void setValue(double value) {
                }

                @Override
                public double getValue() {
                    return 0;
                }
            };
        }

        @Override
        public HistogramMetrics createHistogramMetric(String name, String tips, MetricsTag.Keys tagKeys,
                                                      MetricsTag.Values tagValues, double... buckets) {
            return new HistogramMetrics() {
                @Override
                public void observe(double value) {
                    latencyObservations.add(value);
                }
            };
        }
    }
}
