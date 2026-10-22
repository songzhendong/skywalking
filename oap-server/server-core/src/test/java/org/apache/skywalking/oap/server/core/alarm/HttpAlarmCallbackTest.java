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

import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.GaugeMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpAlarmCallbackTest {

    private static HttpServer SERVER;
    private static CapturingAppender APPENDER;

    /**
     * {@link HttpAlarmCallback#post} reports delivery only through the log, so the status handling can only be
     * observed by capturing what it logs.
     */
    private static class CapturingAppender extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        CapturingAppender() {
            super("capturing", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }

    private static final class Callback extends HttpAlarmCallback {
        Callback() {
            this(Duration.ofSeconds(12));
        }

        Callback(final Duration timeout) {
            super(HttpAlarmCallback.newHttpClient(timeout), timeout);
        }

        Callback(final Duration timeout, final AlarmHttpDeliveryMetrics metrics, final String hookType) {
            super(HttpAlarmCallback.newHttpClient(timeout), timeout, metrics, hookType);
        }

        @Override
        protected void doAlarmCallback(List<AlarmMessage> alarmMessages, boolean isRecovery) {
        }

        String send(int status) throws IOException, InterruptedException {
            return post(
                URI.create("http://127.0.0.1:" + SERVER.getAddress().getPort() + "/status/" + status),
                "{}", Map.of()
            );
        }

        String send(final URI uri) throws IOException, InterruptedException {
            return post(uri, "{}", Map.of());
        }
    }

    private static final class RecordingCreator implements MetricsCreator {
        private final AtomicInteger deliveryIncrements = new AtomicInteger();
        private final AtomicInteger statusIncrements = new AtomicInteger();
        private final List<String> deliveryKeys = new CopyOnWriteArrayList<>();
        private final List<String> statusKeys = new CopyOnWriteArrayList<>();
        private final List<Double> latencies = new ArrayList<>();

        @Override
        public CounterMetrics createCounter(String name, String tips, MetricsTag.Keys tagKeys,
                                            MetricsTag.Values tagValues) {
            final String joined = String.join(",", tagValues.getValues());
            return new CounterMetrics() {
                @Override
                public void inc() {
                    if ("alarm_http_delivery_total".equals(name)) {
                        deliveryIncrements.incrementAndGet();
                        deliveryKeys.add(joined);
                    } else if ("alarm_http_delivery_status_total".equals(name)) {
                        statusIncrements.incrementAndGet();
                        statusKeys.add(joined);
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
                    latencies.add(value);
                }
            };
        }
    }

    @BeforeAll
    public static void setUp() throws IOException {
        SERVER = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        SERVER.createContext("/status", exchange -> {
            final String path = exchange.getRequestURI().getPath();
            final int status = Integer.parseInt(path.substring(path.lastIndexOf('/') + 1));
            final byte[] body = "{\"status\":\"success\"}".getBytes();
            exchange.sendResponseHeaders(status, status == 204 ? -1 : body.length);
            if (status != 204) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        SERVER.start();

        APPENDER = new CapturingAppender();
        APPENDER.start();
        ((Logger) LoggerContext.getContext(false).getLogger(Callback.class.getName())).addAppender(APPENDER);
    }

    @AfterAll
    public static void tearDown() {
        ((Logger) LoggerContext.getContext(false).getLogger(Callback.class.getName())).removeAppender(APPENDER);
        APPENDER.stop();
        SERVER.stop(0);
    }

    @BeforeEach
    public void reset() {
        APPENDER.events.clear();
    }

    @Test
    public void acceptedStatusesAreNotLoggedAsFailure() throws Exception {
        // 202 is what an asynchronous intake API such as PagerDuty's Events API v2 answers with
        for (int status : new int[] {
            200,
            201,
            202,
            204
        }) {
            new Callback().send(status);
        }
        assertTrue(
            APPENDER.events.isEmpty(),
            "a 2xx response means the hook accepted the alarm and must not be logged as a failure, but got: "
                + APPENDER.events
        );
    }

    @Test
    public void nonSuccessStatusIsLoggedAsFailure() throws Exception {
        new Callback().send(500);
        assertEquals(1, APPENDER.events.size());
        assertEquals(Level.ERROR, APPENDER.events.get(0).getLevel());
        assertTrue(APPENDER.events.get(0).getMessage().getFormattedMessage().contains("500"));
    }

    @Test
    public void successAndHttpErrorAreRecorded() throws Exception {
        final RecordingCreator creator = new RecordingCreator();
        final AlarmHttpDeliveryMetrics metrics = new AlarmHttpDeliveryMetrics(creator);
        final Callback callback = new Callback(Duration.ofSeconds(12), metrics, "webhook");

        callback.send(202);
        callback.send(500);

        assertEquals(2, creator.deliveryIncrements.get());
        assertEquals(2, creator.statusIncrements.get());
        assertEquals(2, creator.latencies.size());
        assertTrue(creator.deliveryKeys.stream().anyMatch(k -> k.contains("success")));
        assertTrue(creator.deliveryKeys.stream().anyMatch(k -> k.contains("http_error")));
        assertTrue(creator.statusKeys.stream().anyMatch(k -> k.contains("2xx")));
        assertTrue(creator.statusKeys.stream().anyMatch(k -> k.contains("5xx")));
    }

    @Test
    public void eachPostIncrementsDeliveryCounterOnce() throws Exception {
        final RecordingCreator creator = new RecordingCreator();
        final AlarmHttpDeliveryMetrics metrics = new AlarmHttpDeliveryMetrics(creator);
        final Callback callback = new Callback(Duration.ofSeconds(12), metrics, "webhook");

        for (int i = 0; i < 3; i++) {
            callback.send(200);
        }
        callback.send(500);

        assertEquals(4, creator.deliveryIncrements.get());
        assertEquals(3, creator.deliveryKeys.stream().filter(k -> k.contains("success")).count());
        assertEquals(1, creator.deliveryKeys.stream().filter(k -> k.contains("http_error")).count());
        assertEquals(4, creator.statusIncrements.get());
        assertEquals(4, creator.latencies.size());
    }

    @Test
    public void timeoutIsRecordedWithoutStatusClass() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final HttpServer stalling = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stalling.setExecutor(executor);
        stalling.createContext("/stall-metrics", exchange -> {
            exchange.sendResponseHeaders(200, 64);
            exchange.getResponseBody().write("ping".getBytes(StandardCharsets.UTF_8));
            exchange.getResponseBody().flush();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        stalling.start();
        try {
            final RecordingCreator creator = new RecordingCreator();
            final AlarmHttpDeliveryMetrics metrics = new AlarmHttpDeliveryMetrics(creator);
            final Callback callback = new Callback(Duration.ofSeconds(1), metrics, "slack");
            final URI uri = URI.create("http://127.0.0.1:" + stalling.getAddress().getPort() + "/stall-metrics");
            assertThrows(HttpTimeoutException.class, () -> callback.send(uri));
            assertEquals(1, creator.deliveryIncrements.get());
            assertEquals(0, creator.statusIncrements.get());
            assertTrue(creator.deliveryKeys.stream().anyMatch(k -> k.contains("timeout")));
            assertEquals(1, creator.latencies.size());
        } finally {
            release.countDown();
            stalling.stop(0);
            executor.shutdownNow();
        }
    }

    @Test
    public void stalledResponseBodyTimesOut() throws Exception {
        // The request's own timeout stops at the response headers, so only the delivery deadline bounds the body.
        final CountDownLatch release = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final HttpServer stalling = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stalling.setExecutor(executor);
        stalling.createContext("/stall", exchange -> {
            exchange.sendResponseHeaders(200, 64);
            exchange.getResponseBody().write("ping".getBytes(StandardCharsets.UTF_8));
            exchange.getResponseBody().flush();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        stalling.start();
        try {
            final URI uri = URI.create("http://127.0.0.1:" + stalling.getAddress().getPort() + "/stall");
            final long started = System.nanoTime();
            assertThrows(HttpTimeoutException.class, () -> new Callback(Duration.ofSeconds(1)).send(uri));
            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(elapsedMs < 5_000, "elapsedMs=" + elapsedMs);
        } finally {
            release.countDown();
            stalling.stop(0);
            executor.shutdownNow();
        }
    }
}
