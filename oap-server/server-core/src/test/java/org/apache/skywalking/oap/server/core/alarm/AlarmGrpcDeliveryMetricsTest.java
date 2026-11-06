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
import io.grpc.StatusRuntimeException;
import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.GaugeMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;
import org.apache.skywalking.oap.server.telemetry.none.MetricsCreatorNoop;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AlarmGrpcDeliveryMetricsTest {

    @Test
    public void resultOfMapsGrpcStatus() {
        assertEquals(
            AlarmGrpcDeliveryMetrics.RESULT_TIMEOUT,
            AlarmGrpcDeliveryMetrics.resultOf(new StatusRuntimeException(Status.DEADLINE_EXCEEDED))
        );
        assertEquals(
            AlarmGrpcDeliveryMetrics.RESULT_UNAVAILABLE,
            AlarmGrpcDeliveryMetrics.resultOf(new StatusRuntimeException(Status.UNAVAILABLE))
        );
        assertEquals(
            AlarmGrpcDeliveryMetrics.RESULT_INTERRUPTED,
            AlarmGrpcDeliveryMetrics.resultOf(new StatusRuntimeException(Status.CANCELLED))
        );
        assertEquals(
            AlarmGrpcDeliveryMetrics.RESULT_ERROR,
            AlarmGrpcDeliveryMetrics.resultOf(new StatusRuntimeException(Status.INTERNAL))
        );
        assertEquals(
            AlarmGrpcDeliveryMetrics.RESULT_ERROR,
            AlarmGrpcDeliveryMetrics.resultOf(new RuntimeException("boom"))
        );
    }

    @Test
    public void recordIncrementsCountersAndObservesLatency() {
        final RecordingCreator creator = new RecordingCreator();
        final AlarmGrpcDeliveryMetrics metrics = new AlarmGrpcDeliveryMetrics(creator);

        metrics.record(AlarmGrpcDeliveryMetrics.KIND_ALARM, AlarmGrpcDeliveryMetrics.RESULT_SUCCESS, 3, 0.12);
        metrics.record(AlarmGrpcDeliveryMetrics.KIND_RECOVERY, AlarmGrpcDeliveryMetrics.RESULT_UNAVAILABLE, 2, 0.2);

        assertEquals(2, creator.deliveryIncrements.get());
        assertEquals(5.0, creator.messageIncrements.sum(), 0.0);
        assertEquals(2, creator.latencyObservations.size());
        assertTrue(creator.latencyObservations.stream().allMatch(v -> v >= 0));
    }

    @Test
    public void recordSwallowsCreatorFailures() {
        final MetricsCreator broken = new MetricsCreator() {
            @Override
            public CounterMetrics createCounter(final String name, final String tips, final MetricsTag.Keys tagKeys,
                                                final MetricsTag.Values tagValues) {
                throw new IllegalStateException("boom");
            }

            @Override
            public GaugeMetrics createGauge(final String name, final String tips, final MetricsTag.Keys tagKeys,
                                            final MetricsTag.Values tagValues) {
                throw new IllegalStateException("boom");
            }

            @Override
            public HistogramMetrics createHistogramMetric(final String name, final String tips,
                                                          final MetricsTag.Keys tagKeys,
                                                          final MetricsTag.Values tagValues, final double... buckets) {
                throw new IllegalStateException("boom");
            }
        };

        new AlarmGrpcDeliveryMetrics(broken)
            .record(AlarmGrpcDeliveryMetrics.KIND_ALARM, AlarmGrpcDeliveryMetrics.RESULT_SUCCESS, 1, 0.01);
    }

    @Test
    public void negativeLatencyIsClamped() {
        final RecordingCreator creator = new RecordingCreator();
        final AlarmGrpcDeliveryMetrics metrics = new AlarmGrpcDeliveryMetrics(creator);

        metrics.record(AlarmGrpcDeliveryMetrics.KIND_ALARM, AlarmGrpcDeliveryMetrics.RESULT_SUCCESS, 1, -1.5);

        assertEquals(1, creator.deliveryIncrements.get());
        assertEquals(1, creator.latencyObservations.size());
        assertEquals(0.0, creator.latencyObservations.get(0), 0.0);
    }

    @Test
    public void repeatedRecordsReuseTheSameCounterInstance() {
        final RecordingCreator creator = new RecordingCreator();
        final AlarmGrpcDeliveryMetrics metrics = new AlarmGrpcDeliveryMetrics(creator);

        metrics.record(AlarmGrpcDeliveryMetrics.KIND_ALARM, AlarmGrpcDeliveryMetrics.RESULT_SUCCESS, 1, 0.1);
        metrics.record(AlarmGrpcDeliveryMetrics.KIND_ALARM, AlarmGrpcDeliveryMetrics.RESULT_SUCCESS, 2, 0.2);

        assertEquals(1, creator.deliveryCreates.get());
        assertEquals(1, creator.messageCreates.get());
        assertEquals(1, creator.histogramCreates.get());
        assertEquals(2, creator.deliveryIncrements.get());
        assertEquals(3.0, creator.messageIncrements.sum(), 0.0);
        assertEquals(2, creator.latencyObservations.size());
    }

    private static final class RecordingCreator extends MetricsCreatorNoop {
        private final AtomicInteger deliveryIncrements = new AtomicInteger();
        private final AtomicInteger deliveryCreates = new AtomicInteger();
        private final AtomicInteger messageCreates = new AtomicInteger();
        private final AtomicInteger histogramCreates = new AtomicInteger();
        private final DoubleAdder messageIncrements = new DoubleAdder();
        private final List<Double> latencyObservations = new ArrayList<>();
        private final List<String> deliveryKeys = new CopyOnWriteArrayList<>();

        @Override
        public CounterMetrics createCounter(final String name, final String tips, final MetricsTag.Keys tagKeys,
                                            final MetricsTag.Values tagValues) {
            final String joined = String.join(",", tagValues.getValues());
            if ("alarm_grpc_delivery_total".equals(name)) {
                deliveryCreates.incrementAndGet();
            } else if ("alarm_grpc_delivery_messages_total".equals(name)) {
                messageCreates.incrementAndGet();
            }
            return new CounterMetrics() {
                @Override
                public void inc() {
                    inc(1);
                }

                @Override
                public void inc(final double value) {
                    if ("alarm_grpc_delivery_total".equals(name)) {
                        deliveryIncrements.incrementAndGet();
                        deliveryKeys.add(joined);
                    } else if ("alarm_grpc_delivery_messages_total".equals(name)) {
                        messageIncrements.add(value);
                    }
                }
            };
        }

        @Override
        public HistogramMetrics createHistogramMetric(final String name, final String tips,
                                                      final MetricsTag.Keys tagKeys,
                                                      final MetricsTag.Values tagValues, final double... buckets) {
            histogramCreates.incrementAndGet();
            return new HistogramMetrics() {
                @Override
                public void observe(final double value) {
                    latencyObservations.add(value);
                }
            };
        }
    }
}
