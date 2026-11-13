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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.GaugeMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RemoteClientMetricsTest {

    @Test
    public void reasonOfMapsGrpcStatus() {
        Assertions.assertEquals(RemoteClientMetrics.REASON_TIMEOUT, RemoteClientMetrics.reasonOf(Status.DEADLINE_EXCEEDED));
        Assertions.assertEquals(RemoteClientMetrics.REASON_IO_ERROR, RemoteClientMetrics.reasonOf(Status.UNAVAILABLE));
        Assertions.assertEquals(RemoteClientMetrics.REASON_RATE_LIMITED, RemoteClientMetrics.reasonOf(Status.RESOURCE_EXHAUSTED));
        Assertions.assertEquals(RemoteClientMetrics.REASON_AUTH_FAILED, RemoteClientMetrics.reasonOf(Status.UNAUTHENTICATED));
        Assertions.assertEquals(RemoteClientMetrics.REASON_AUTH_FAILED, RemoteClientMetrics.reasonOf(Status.PERMISSION_DENIED));
        Assertions.assertEquals(RemoteClientMetrics.REASON_SERVER_ERROR, RemoteClientMetrics.reasonOf(Status.INTERNAL));
        Assertions.assertEquals(RemoteClientMetrics.REASON_REJECTED, RemoteClientMetrics.reasonOf(Status.CANCELLED));
        Assertions.assertEquals(RemoteClientMetrics.REASON_ERROR, RemoteClientMetrics.reasonOf(null));
    }

    @Test
    public void forGrpcRecordsLabeledErrorsAndLatency() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        final RemoteClientMetrics metrics = RemoteClientMetrics.forGrpc(creator, new Address("oap2", 11800, false));

        metrics.incOut();
        metrics.observeLatencySeconds(0.2);
        metrics.recordError(RemoteClientMetrics.REASON_RATE_LIMITED);
        metrics.setStreamInflight(3);
        metrics.addStreamBlockSeconds(1.5);
        metrics.setQueueUsedPercentage(42);

        Assertions.assertEquals(1, creator.counterIncs.get("remote_out_count"));
        Assertions.assertEquals(1, creator.counterIncs.get("remote_out_error_count"));
        Assertions.assertTrue(creator.lastErrorTags.get().contains("rate_limited"));
        Assertions.assertEquals(1, creator.latencyObservations.get());
        Assertions.assertEquals(3.0, creator.gaugeValues.get("remote_out_stream_inflight"), 0.01);
        Assertions.assertEquals(1.5, creator.counterIncAmounts.get("remote_out_stream_block_seconds"), 0.01);
        Assertions.assertEquals(42.0, creator.gaugeValues.get("remote_out_queue_used_percentage"), 0.01);
    }

    @Test
    public void forSelfSkipsStreamAndQueueSeries() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        RemoteClientMetrics.forSelf(creator, new Address("oap1", 11800, true));
        Assertions.assertFalse(creator.createdNames.contains("remote_out_stream_inflight"));
        Assertions.assertFalse(creator.createdNames.contains("remote_out_queue_used_percentage"));
        Assertions.assertTrue(creator.createdNames.contains("remote_out_count"));
        Assertions.assertTrue(creator.createdNames.contains("remote_out_latency"));
    }

    @Test
    public void clearPressureGaugesZerosStreamAndQueue() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        final RemoteClientMetrics metrics = RemoteClientMetrics.forGrpc(creator, new Address("oap2", 11800, false));
        metrics.setStreamInflight(4);
        metrics.setQueueUsedPercentage(77);
        metrics.clearPressureGauges();
        Assertions.assertEquals(0.0, creator.gaugeValues.get("remote_out_stream_inflight"), 0.01);
        Assertions.assertEquals(0.0, creator.gaugeValues.get("remote_out_queue_used_percentage"), 0.01);
    }

    @Test
    public void queueUsedPercentageIsClamped() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        final RemoteClientMetrics metrics = RemoteClientMetrics.forGrpc(creator, new Address("oap2", 11800, false));
        metrics.setQueueUsedPercentage(-5);
        Assertions.assertEquals(0.0, creator.gaugeValues.get("remote_out_queue_used_percentage"), 0.01);
        metrics.setQueueUsedPercentage(150);
        Assertions.assertEquals(100.0, creator.gaugeValues.get("remote_out_queue_used_percentage"), 0.01);
    }

    @Test
    public void zeroBlockSecondsAreIgnoredAfterSeed() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        final RemoteClientMetrics metrics = RemoteClientMetrics.forGrpc(creator, new Address("oap2", 11800, false));
        // Construction seeds the Prometheus child with inc(0) so scrapes always see the series.
        Assertions.assertEquals(0.0, creator.counterIncAmounts.get("remote_out_stream_block_seconds"), 0.01);
        metrics.addStreamBlockSeconds(0);
        metrics.addStreamBlockSeconds(-1);
        Assertions.assertEquals(0.0, creator.counterIncAmounts.get("remote_out_stream_block_seconds"), 0.01);
    }

    @Test
    public void reasonOfCoversAdditionalServerFailures() {
        Assertions.assertEquals(RemoteClientMetrics.REASON_SERVER_ERROR, RemoteClientMetrics.reasonOf(Status.UNKNOWN));
        Assertions.assertEquals(RemoteClientMetrics.REASON_SERVER_ERROR, RemoteClientMetrics.reasonOf(Status.DATA_LOSS));
        Assertions.assertEquals(RemoteClientMetrics.REASON_SERVER_ERROR, RemoteClientMetrics.reasonOf(Status.UNIMPLEMENTED));
        Assertions.assertEquals(RemoteClientMetrics.REASON_REJECTED, RemoteClientMetrics.reasonOf(Status.INVALID_ARGUMENT));
    }

    @Test
    public void recordErrorNullFallsBackToError() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        final RemoteClientMetrics metrics = RemoteClientMetrics.forGrpc(creator, new Address("oap2", 11800, false));
        metrics.recordError(null);
        metrics.recordError("");
        Assertions.assertEquals(2, creator.counterIncs.get("remote_out_error_count"));
        Assertions.assertTrue(creator.lastErrorTags.get().endsWith("error"));
    }

    @Test
    public void interruptedReasonIsRecordedDirectly() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        final RemoteClientMetrics metrics = RemoteClientMetrics.forGrpc(creator, new Address("oap2", 11800, false));
        metrics.recordError(RemoteClientMetrics.REASON_INTERRUPTED);
        Assertions.assertEquals(1, creator.counterIncs.get("remote_out_error_count"));
        Assertions.assertTrue(creator.lastErrorTags.get().contains(RemoteClientMetrics.REASON_INTERRUPTED));
    }

    @Test
    public void negativeLatencyIsClamped() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        final RemoteClientMetrics metrics = RemoteClientMetrics.forGrpc(creator, new Address("oap2", 11800, false));
        metrics.observeLatencySeconds(-1);
        Assertions.assertEquals(1, creator.latencyObservations.get());
        Assertions.assertEquals(0.0, creator.lastLatencyValue.get(), 0.01);
    }

    @Test
    public void streamInflightIsClampedAtZero() {
        final RecordingMetricsCreator creator = new RecordingMetricsCreator();
        final RemoteClientMetrics metrics = RemoteClientMetrics.forGrpc(creator, new Address("oap2", 11800, false));
        metrics.setStreamInflight(-3);
        Assertions.assertEquals(0.0, creator.gaugeValues.get("remote_out_stream_inflight"), 0.01);
    }

    @Test
    public void forSelfAndForGrpcUseDistinctSelfTags() {
        final RecordingMetricsCreator selfCreator = new RecordingMetricsCreator();
        final RecordingMetricsCreator peerCreator = new RecordingMetricsCreator();
        RemoteClientMetrics.forSelf(selfCreator, new Address("oap1", 11800, true));
        RemoteClientMetrics.forGrpc(peerCreator, new Address("oap2", 11800, false));
        Assertions.assertTrue(selfCreator.createdTagSets.contains("oap1_11800,Y"));
        Assertions.assertTrue(peerCreator.createdTagSets.contains("oap2_11800,N"));
        Assertions.assertFalse(selfCreator.createdTagSets.contains("oap1_11800,N"));
    }

    private static final class RecordingMetricsCreator implements MetricsCreator {
        private final List<String> createdNames = new ArrayList<>();
        private final List<String> createdTagSets = new ArrayList<>();
        private final java.util.Map<String, Integer> counterIncs = new java.util.HashMap<>();
        private final java.util.Map<String, Double> counterIncAmounts = new java.util.HashMap<>();
        private final java.util.Map<String, Double> gaugeValues = new java.util.HashMap<>();
        private final AtomicInteger latencyObservations = new AtomicInteger();
        private final AtomicReference<String> lastErrorTags = new AtomicReference<>("");
        private final AtomicReference<Double> lastLatencyValue = new AtomicReference<>();

        @Override
        public CounterMetrics createCounter(String name, String tips, MetricsTag.Keys tagKeys, MetricsTag.Values tagValues) {
            createdNames.add(name);
            final String tagJoined = String.join(",", tagValues.getValues());
            createdTagSets.add(tagJoined);
            return new CounterMetrics() {
                @Override
                public void inc() {
                    counterIncs.merge(name, 1, Integer::sum);
                    if ("remote_out_error_count".equals(name)) {
                        lastErrorTags.set(tagJoined);
                    }
                }

                @Override
                public void inc(double value) {
                    counterIncAmounts.merge(name, value, Double::sum);
                }
            };
        }

        @Override
        public GaugeMetrics createGauge(String name, String tips, MetricsTag.Keys tagKeys, MetricsTag.Values tagValues) {
            createdNames.add(name);
            createdTagSets.add(String.join(",", tagValues.getValues()));
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
                    gaugeValues.put(name, value);
                }

                @Override
                public double getValue() {
                    return gaugeValues.getOrDefault(name, 0d);
                }
            };
        }

        @Override
        public HistogramMetrics createHistogramMetric(String name, String tips, MetricsTag.Keys tagKeys,
                                                      MetricsTag.Values tagValues, double... buckets) {
            createdNames.add(name);
            createdTagSets.add(String.join(",", tagValues.getValues()));
            return new HistogramMetrics() {
                @Override
                public void observe(double value) {
                    latencyObservations.incrementAndGet();
                    lastLatencyValue.set(value);
                }
            };
        }
    }
}
