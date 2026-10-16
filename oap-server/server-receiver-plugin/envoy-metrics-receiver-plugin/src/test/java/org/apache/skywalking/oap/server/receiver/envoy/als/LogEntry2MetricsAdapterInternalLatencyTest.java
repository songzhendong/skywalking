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

package org.apache.skywalking.oap.server.receiver.envoy.als;

import com.google.protobuf.Duration;
import com.google.protobuf.UInt32Value;
import io.envoyproxy.envoy.data.accesslog.v3.AccessLogCommon;
import io.envoyproxy.envoy.data.accesslog.v3.ConnectionProperties;
import io.envoyproxy.envoy.data.accesslog.v3.HTTPAccessLogEntry;
import io.envoyproxy.envoy.data.accesslog.v3.HTTPResponseProperties;
import io.envoyproxy.envoy.data.accesslog.v3.TCPAccessLogEntry;
import org.apache.skywalking.apm.network.servicemesh.v3.HTTPServiceMeshMetric;
import org.apache.skywalking.apm.network.servicemesh.v3.TCPServiceMeshMetric;
import org.apache.skywalking.oap.server.receiver.envoy.als.tcp.TCPLogEntry2MetricsAdapter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogEntry2MetricsAdapterInternalLatencyTest {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    @Test
    public void formatAsNanosIncludesWholeSeconds() {
        final Duration oneAndHalfSeconds = Duration.newBuilder().setSeconds(1).setNanos(500_000_000).build();
        assertEquals(1_500_000_000L, LogEntry2MetricsAdapter.formatAsNanos(oneAndHalfSeconds));
        // Legacy mistaken path: getNanos() alone drops the second.
        assertEquals(500_000_000, oneAndHalfSeconds.getNanos());
    }

    @Test
    public void httpInternalLatenciesKeepWholeSecondsAndCrossSecondDiff() {
        // request path: 1.5s to first upstream tx
        // response path: first downstream tx 1.1s - first upstream rx 0.9s = 0.2s
        // Old getNanos()-only math: 100_000_000 - 900_000_000 = -800_000_000
        final AccessLogCommon common = AccessLogCommon.newBuilder()
            .setTimeToFirstUpstreamTxByte(duration(1, 500_000_000))
            .setTimeToFirstUpstreamRxByte(duration(0, 900_000_000))
            .setTimeToFirstDownstreamTxByte(duration(1, 100_000_000))
            .build();
        final HTTPAccessLogEntry entry = HTTPAccessLogEntry.newBuilder()
            .setCommonProperties(common)
            .setResponse(HTTPResponseProperties.newBuilder().setResponseCode(UInt32Value.of(200)))
            .build();

        final HTTPServiceMeshMetric.Builder metric = new LogEntry2MetricsAdapter(entry, null, null).adaptCommonPart();

        assertEquals(1_500_000_000L, metric.getInternalRequestLatencyNanos());
        assertEquals(200_000_000L, metric.getInternalResponseLatencyNanos());
    }

    @Test
    public void tcpInternalLatenciesKeepWholeSecondsAndCrossSecondDiff() {
        final AccessLogCommon common = AccessLogCommon.newBuilder()
            .setTimeToFirstUpstreamTxByte(duration(2, 250_000_000))
            .setTimeToFirstUpstreamRxByte(duration(0, 800_000_000))
            .setTimeToLastDownstreamTxByte(duration(1, 100_000_000))
            .build();
        final TCPAccessLogEntry entry = TCPAccessLogEntry.newBuilder()
            .setCommonProperties(common)
            .setConnectionProperties(ConnectionProperties.newBuilder().setReceivedBytes(1).setSentBytes(1))
            .build();

        final TCPServiceMeshMetric.Builder metric = new TCPLogEntry2MetricsAdapter(entry, null, null).adaptCommonPart();

        assertEquals(2_250_000_000L, metric.getInternalRequestLatencyNanos());
        // 1.1s - 0.8s = 0.3s; nanos-only would be 100_000_000 - 800_000_000 = -700_000_000
        assertEquals(300_000_000L, metric.getInternalResponseLatencyNanos());
    }

    @Test
    public void formatAsNanosAcceptsLongExtremesAndRejectsOnePast() {
        final long maxSeconds = Long.MAX_VALUE / NANOS_PER_SECOND;
        final int maxNanos = (int) (Long.MAX_VALUE % NANOS_PER_SECOND);
        assertEquals(Long.MAX_VALUE, LogEntry2MetricsAdapter.formatAsNanos(duration(maxSeconds, maxNanos)));

        final LogEntry2MetricsAdapter.UnrepresentableAlsDurationException aboveMax = assertThrows(
            LogEntry2MetricsAdapter.UnrepresentableAlsDurationException.class,
            () -> LogEntry2MetricsAdapter.formatAsNanos(duration(maxSeconds, maxNanos + 1)));
        assertTrue(aboveMax.getMessage().contains("seconds=" + maxSeconds));
        assertTrue(aboveMax.getMessage().contains("refusing to record a truncated latency"));

        // Java division truncates toward zero, so this pair is the most negative long toNanos() can return.
        final long minSeconds = Long.MIN_VALUE / NANOS_PER_SECOND;
        final int minNanos = (int) (Long.MIN_VALUE % NANOS_PER_SECOND);
        assertEquals(Long.MIN_VALUE, LogEntry2MetricsAdapter.formatAsNanos(duration(minSeconds, minNanos)));
        assertThrows(
            LogEntry2MetricsAdapter.UnrepresentableAlsDurationException.class,
            () -> LogEntry2MetricsAdapter.formatAsNanos(duration(minSeconds, minNanos - 1)));
    }

    @Test
    public void httpKeepsNegativeRepresentableLatency() {
        // 0.1s - 0.9s = -0.8s. The timestamps are out of order, and the difference still fits in a long.
        final AccessLogCommon common = AccessLogCommon.newBuilder()
            .setTimeToFirstUpstreamTxByte(duration(0, 100_000_000))
            .setTimeToFirstUpstreamRxByte(duration(0, 900_000_000))
            .setTimeToFirstDownstreamTxByte(duration(0, 100_000_000))
            .build();
        final HTTPAccessLogEntry entry = HTTPAccessLogEntry.newBuilder()
            .setCommonProperties(common)
            .setResponse(HTTPResponseProperties.newBuilder().setResponseCode(UInt32Value.of(200)))
            .build();

        final HTTPServiceMeshMetric.Builder metric = new LogEntry2MetricsAdapter(entry, null, null).adaptCommonPart();

        assertEquals(-800_000_000L, metric.getInternalResponseLatencyNanos());
    }

    @Test
    public void httpRejectsDurationThatDoesNotFit() {
        final long maxSeconds = Long.MAX_VALUE / NANOS_PER_SECOND;
        final AccessLogCommon common = AccessLogCommon.newBuilder()
            .setTimeToFirstUpstreamTxByte(duration(maxSeconds + 1, 0))
            .build();
        final HTTPAccessLogEntry entry = HTTPAccessLogEntry.newBuilder()
            .setCommonProperties(common)
            .setResponse(HTTPResponseProperties.newBuilder().setResponseCode(UInt32Value.of(200)))
            .build();

        final LogEntry2MetricsAdapter.UnrepresentableAlsDurationException rejected = assertThrows(
            LogEntry2MetricsAdapter.UnrepresentableAlsDurationException.class,
            () -> new LogEntry2MetricsAdapter(entry, null, null).adaptCommonPart());
        assertTrue(rejected.getMessage().contains("refusing to record a truncated latency"));
    }

    @Test
    public void tcpRejectsLatencyDifferenceThatOverflows() {
        final long maxSeconds = Long.MAX_VALUE / NANOS_PER_SECOND;
        final int maxNanos = (int) (Long.MAX_VALUE % NANOS_PER_SECOND);
        final AccessLogCommon common = AccessLogCommon.newBuilder()
            .setTimeToFirstUpstreamTxByte(duration(0, 1))
            .setTimeToFirstUpstreamRxByte(duration(0, -1))
            .setTimeToLastDownstreamTxByte(duration(maxSeconds, maxNanos))
            .build();
        final TCPAccessLogEntry entry = TCPAccessLogEntry.newBuilder()
            .setCommonProperties(common)
            .setConnectionProperties(ConnectionProperties.newBuilder().setReceivedBytes(1).setSentBytes(1))
            .build();

        final LogEntry2MetricsAdapter.UnrepresentableAlsDurationException rejected = assertThrows(
            LogEntry2MetricsAdapter.UnrepresentableAlsDurationException.class,
            () -> new TCPLogEntry2MetricsAdapter(entry, null, null).adaptCommonPart());
        assertTrue(rejected.getMessage().contains("refusing to record a wrapped latency"));
    }

    private static Duration duration(final long seconds, final int nanos) {
        return Duration.newBuilder().setSeconds(seconds).setNanos(nanos).build();
    }
}
