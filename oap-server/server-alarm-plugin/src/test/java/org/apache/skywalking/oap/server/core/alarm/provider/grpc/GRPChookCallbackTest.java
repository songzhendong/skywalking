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

package org.apache.skywalking.oap.server.core.alarm.provider.grpc;

import com.google.common.collect.Lists;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.skywalking.oap.server.core.alarm.AlarmGrpcDeliveryMetrics;
import org.apache.skywalking.oap.server.core.alarm.AlarmMessage;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecoveryMessage;
import org.apache.skywalking.oap.server.core.alarm.grpc.AlarmServiceGrpc;
import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;
import org.apache.skywalking.oap.server.telemetry.none.MetricsCreatorNoop;
import org.apache.skywalking.oap.server.core.alarm.grpc.Response;
import org.apache.skywalking.oap.server.core.alarm.provider.AlarmHooksType;
import org.apache.skywalking.oap.server.core.alarm.provider.AlarmRulesWatcher;
import org.apache.skywalking.oap.server.core.alarm.provider.Rules;
import org.apache.skywalking.oap.server.core.analysis.manual.searchtag.Tag;
import org.apache.skywalking.oap.server.core.query.enumeration.Scope;
import org.apache.skywalking.oap.server.library.client.grpc.GRPCClient;
import org.apache.skywalking.oap.server.testing.util.ReflectUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GRPChookCallbackTest {

    private GRPCCallback grpcCallback;

    private AlarmRulesWatcher alarmRulesWatcher;

    private List<AlarmMessage> alarmMessageList;
    private List<AlarmMessage> alarmRecoveryMessageList;

    @BeforeEach
    public void init() throws Exception {
        Rules rules = new Rules();
        GRPCAlarmSetting setting1 = new GRPCAlarmSetting("setting1", AlarmHooksType.gRPC, true);
        setting1.setTargetHost("127.0.0.1");
        setting1.setTargetPort(9888);
        GRPCAlarmSetting setting2 = new GRPCAlarmSetting("setting2", AlarmHooksType.gRPC, false);
        setting2.setTargetHost("127.0.0.1");
        setting2.setTargetPort(9888);
        rules.getGrpcAlarmSettingMap().put(setting1.getFormattedName(), setting1);
        rules.getGrpcAlarmSettingMap().put(setting2.getFormattedName(), setting2);

        alarmRulesWatcher = new AlarmRulesWatcher(rules, null, null);
        grpcCallback = new GRPCCallback(alarmRulesWatcher, noopMetrics());
        mockAlarmMessage(setting1.getFormattedName(), setting2.getFormattedName());
        mockAlarmRecoveryMessage(setting1.getFormattedName(), setting2.getFormattedName());
    }

    @Test
    public void doAlarm() {
        grpcCallback.doAlarm(alarmMessageList);
        grpcCallback.doAlarmRecovery(alarmRecoveryMessageList);
    }

    @Test
    public void testGauchoSettingClean() {
        Rules rules = new Rules();
        GRPCAlarmSetting setting1 = new GRPCAlarmSetting("setting1111111", AlarmHooksType.gRPC, true);
        GRPCAlarmSetting setting2 = new GRPCAlarmSetting("setting2222222", AlarmHooksType.gRPC, true);
        rules.getGrpcAlarmSettingMap().put(setting1.getFormattedName(), setting1);
        rules.getGrpcAlarmSettingMap().put(setting2.getFormattedName(), setting2);
        alarmRulesWatcher = new AlarmRulesWatcher(rules, null, null);
        grpcCallback = new GRPCCallback(alarmRulesWatcher, noopMetrics());
        grpcCallback.doAlarm(alarmMessageList);
        grpcCallback.doAlarmRecovery(alarmRecoveryMessageList);
    }

    @Test
    public void shouldRecordSuccessMetricsAgainstLiveServer() throws Exception {
        final int port = freePort();
        final RecordingAlarmHandler handler = new RecordingAlarmHandler();
        final Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
            .addService(handler)
            .build()
            .start();
        final RecordingGrpcMetrics metrics = new RecordingGrpcMetrics();
        try {
            Rules rules = new Rules();
            GRPCAlarmSetting setting = setting("metrics-hook", true, port);
            rules.getGrpcAlarmSettingMap().put(setting.getFormattedName(), setting);
            AlarmRulesWatcher watcher = new AlarmRulesWatcher(rules, null, null);
            GRPCCallback callback = new GRPCCallback(watcher, metrics.metrics);

            AlarmMessage alarm = alarmMessage(setting.getFormattedName(), "metrics-alarm");
            callback.doAlarm(Collections.singletonList(alarm));
            callback.doAlarmRecovery(Collections.singletonList(new AlarmRecoveryMessage(alarm)));

            assertEquals(1, metrics.successAlarmDeliveries.get());
            assertEquals(1, metrics.successRecoveryDeliveries.get());
            assertEquals(1, metrics.successAlarmMessages.get());
            assertEquals(1, metrics.successRecoveryMessages.get());
            assertEquals(2, metrics.latencyObservations.get());
            assertTrue(metrics.lastLatencySeconds.get() >= 0);

            watcher.notify(new Rules());
            callback.doAlarm(Collections.emptyList());
        } finally {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void interruptedWinsOverStreamTimeoutWhenClassifyingResult() {
        final StatusRuntimeException deadline =
            new StatusRuntimeException(Status.DEADLINE_EXCEEDED.withDescription("deadline"));
        assertEquals(
            AlarmGrpcDeliveryMetrics.RESULT_INTERRUPTED,
            GRPCCallback.classifyDeliveryResult(true, deadline)
        );
        assertEquals(
            AlarmGrpcDeliveryMetrics.RESULT_TIMEOUT,
            GRPCCallback.classifyDeliveryResult(false, deadline)
        );
        assertEquals(
            AlarmGrpcDeliveryMetrics.RESULT_SUCCESS,
            GRPCCallback.classifyDeliveryResult(false, null)
        );
    }

    @Test
    public void shouldRecordUnavailableWhenTargetRefuses() throws Exception {
        final int closedPort = freePort();
        final RecordingGrpcMetrics metrics = new RecordingGrpcMetrics();
        Rules rules = new Rules();
        GRPCAlarmSetting setting = setting("down-hook", true, closedPort);
        rules.getGrpcAlarmSettingMap().put(setting.getFormattedName(), setting);
        AlarmRulesWatcher watcher = new AlarmRulesWatcher(rules, null, null);
        GRPCCallback callback = new GRPCCallback(watcher, metrics.metrics);

        AlarmMessage alarm = alarmMessage(setting.getFormattedName(), "down-alarm");
        callback.doAlarm(Collections.singletonList(alarm));

        assertEquals(1, metrics.unavailableDeliveries.get());
        assertEquals(1, metrics.unavailableMessages.get());
        assertEquals(0, metrics.errorDeliveries.get());
        assertEquals(0, metrics.timeoutDeliveries.get());
        assertEquals(1, metrics.latencyObservations.get());

        watcher.notify(new Rules());
        callback.doAlarm(Collections.emptyList());
    }

    /**
     * Dynamic config clearing gRPC hooks used to null the stub/client maps; restoring
     * targets then NPE'd on the next alarm. After restore, real alarm and recovery
     * messages must reach a live hook server — not only leave maps non-empty.
     */
    @Test
    public void shouldKeepMapsUsableAfterClearThenRestore() throws Exception {
        final int port = freePort();
        final RecordingAlarmHandler handler = new RecordingAlarmHandler();
        final Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
            .addService(handler)
            .build()
            .start();
        try {
            Rules initial = new Rules();
            GRPCAlarmSetting setting = setting("restore-hook", true, port);
            initial.getGrpcAlarmSettingMap().put(setting.getFormattedName(), setting);

            AlarmRulesWatcher watcher = new AlarmRulesWatcher(initial, null, null);
            GRPCCallback callback = new GRPCCallback(watcher, noopMetrics());

            watcher.notify(new Rules());
            assertDoesNotThrow(() -> callback.doAlarm(Collections.emptyList()));
            assertThat(ReflectUtil.<Map<?, ?>>getInternalState(callback, "alarmServiceStubMap")).isNotNull().isEmpty();
            assertThat(ReflectUtil.<Map<?, ?>>getInternalState(callback, "grpcClientMap")).isNotNull().isEmpty();

            Rules restored = new Rules();
            GRPCAlarmSetting restoredSetting = setting("restore-hook", true, port);
            restored.getGrpcAlarmSettingMap().put(restoredSetting.getFormattedName(), restoredSetting);
            watcher.notify(restored);

            AlarmMessage alarm = alarmMessage(setting.getFormattedName(), "restored-alarm");
            AlarmRecoveryMessage recovery = new AlarmRecoveryMessage(alarm);
            assertDoesNotThrow(() -> callback.doAlarm(Collections.singletonList(alarm)));
            assertDoesNotThrow(() -> callback.doAlarmRecovery(Collections.singletonList(recovery)));

            assertThat(ReflectUtil.<Map<?, ?>>getInternalState(callback, "alarmServiceStubMap")).isNotEmpty();
            assertThat(ReflectUtil.<Map<?, ?>>getInternalState(callback, "grpcClientMap")).isNotEmpty();
            assertThat(handler.alarms).extracting(org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage::getName)
                .containsExactly("restored-alarm");
            assertThat(handler.recoveries)
                .extracting(org.apache.skywalking.oap.server.core.alarm.grpc.AlarmRecoveryMessage::getName)
                .containsExactly("restored-alarm");

            watcher.notify(new Rules());
            callback.doAlarm(Collections.emptyList());
        } finally {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A non-empty update that drops a hook must shut down that hook's client/stub, while
     * an unchanged host/port keeps the existing client instance.
     */
    @Test
    public void shouldShutdownClientWhenHookRemovedFromNonEmptyConfig() throws IOException {
        final int port = freePort();
        Rules initial = new Rules();
        GRPCAlarmSetting keep = setting("keep-hook", true, port);
        GRPCAlarmSetting drop = setting("drop-hook", false, port);
        initial.getGrpcAlarmSettingMap().put(keep.getFormattedName(), keep);
        initial.getGrpcAlarmSettingMap().put(drop.getFormattedName(), drop);

        AlarmRulesWatcher watcher = new AlarmRulesWatcher(initial, null, null);
        GRPCCallback callback = new GRPCCallback(watcher, noopMetrics());
        final GRPCClient keptClient = ReflectUtil.<Map<String, GRPCClient>>getInternalState(
            callback, "grpcClientMap").get(keep.getFormattedName());
        final GRPCClient droppedClient = ReflectUtil.<Map<String, GRPCClient>>getInternalState(
            callback, "grpcClientMap").get(drop.getFormattedName());
        assertThat(keptClient).isNotNull();
        assertThat(droppedClient).isNotNull();

        Rules shrunk = new Rules();
        GRPCAlarmSetting keepOnly = setting("keep-hook", true, port);
        shrunk.getGrpcAlarmSettingMap().put(keepOnly.getFormattedName(), keepOnly);
        watcher.notify(shrunk);
        callback.doAlarm(Collections.emptyList());

        Map<String, GRPCClient> clients = ReflectUtil.getInternalState(callback, "grpcClientMap");
        assertThat(clients.keySet()).containsExactly(keep.getFormattedName());
        assertThat(clients.get(keep.getFormattedName())).isSameAs(keptClient);
        assertThat(ReflectUtil.<Map<String, ?>>getInternalState(callback, "alarmServiceStubMap").keySet())
            .containsExactly(keep.getFormattedName());
        assertThat(droppedClient.getChannel().isShutdown()).isTrue();

        watcher.notify(new Rules());
        callback.doAlarm(Collections.emptyList());
    }

    /**
     * Re-parsing the same host/port into a new settings object must not rebuild the client.
     */
    @Test
    public void shouldKeepClientWhenHostPortUnchangedAcrossReparse() throws IOException {
        final int port = freePort();
        Rules initial = new Rules();
        GRPCAlarmSetting setting = setting("stable-hook", true, port);
        initial.getGrpcAlarmSettingMap().put(setting.getFormattedName(), setting);

        AlarmRulesWatcher watcher = new AlarmRulesWatcher(initial, null, null);
        GRPCCallback callback = new GRPCCallback(watcher, noopMetrics());
        final GRPCClient original = ReflectUtil.<Map<String, GRPCClient>>getInternalState(
            callback, "grpcClientMap").get(setting.getFormattedName());

        Rules reparsed = new Rules();
        GRPCAlarmSetting sameTarget = setting("stable-hook", false, port);
        reparsed.getGrpcAlarmSettingMap().put(sameTarget.getFormattedName(), sameTarget);
        watcher.notify(reparsed);
        callback.doAlarm(Collections.emptyList());

        assertThat(ReflectUtil.<Map<String, GRPCClient>>getInternalState(callback, "grpcClientMap")
            .get(setting.getFormattedName())).isSameAs(original);

        watcher.notify(new Rules());
        callback.doAlarm(Collections.emptyList());
    }

    /**
     * A null value in the dynamic settings map must not NPE the rebuild path.
     */
    @Test
    public void shouldIgnoreNullSettingEntryWithoutThrowing() throws IOException {
        final int port = freePort();
        Rules initial = new Rules();
        GRPCAlarmSetting keep = setting("keep-hook", true, port);
        initial.getGrpcAlarmSettingMap().put(keep.getFormattedName(), keep);

        AlarmRulesWatcher watcher = new AlarmRulesWatcher(initial, null, null);
        GRPCCallback callback = new GRPCCallback(watcher, noopMetrics());

        Rules dirty = new Rules();
        dirty.getGrpcAlarmSettingMap().put(keep.getFormattedName(), keep);
        dirty.getGrpcAlarmSettingMap().put("null-hook.grpc", null);
        watcher.notify(dirty);

        assertDoesNotThrow(() -> callback.doAlarm(Collections.emptyList()));
        assertThat(ReflectUtil.<Map<String, ?>>getInternalState(callback, "grpcClientMap").keySet())
            .containsExactly(keep.getFormattedName());

        watcher.notify(new Rules());
        callback.doAlarm(Collections.emptyList());
    }

    /**
     * Alarm messages that name a hook missing from settings must not NPE, and must still
     * count as a failed delivery (same outage visibility as a WeLink token failure).
     */
    @Test
    public void shouldRecordErrorWhenMessageHookMissingFromSettings() throws IOException {
        final int port = freePort();
        Rules rules = new Rules();
        GRPCAlarmSetting keep = setting("keep-hook", true, port);
        rules.getGrpcAlarmSettingMap().put(keep.getFormattedName(), keep);

        final RecordingGrpcMetrics metrics = new RecordingGrpcMetrics();
        AlarmRulesWatcher watcher = new AlarmRulesWatcher(rules, null, null);
        GRPCCallback callback = new GRPCCallback(watcher, metrics.metrics);

        AlarmMessage message = new AlarmMessage();
        message.setId0("1");
        message.setId1("2");
        message.setScope(Scope.Service.name());
        message.setName("orphan hook message");
        message.setAlarmMessage("message");
        message.setRuleName("mock_rule");
        message.setStartTime(System.currentTimeMillis());
        message.setTags(Arrays.asList(new Tag("key", "value")));
        // Formatted name is type + '.' + id (AlarmHookSettings.getFormattedName).
        message.getHooks().add(AlarmHooksType.gRPC.name() + ".missing-hook");

        assertDoesNotThrow(() -> callback.doAlarm(Collections.singletonList(message)));
        assertEquals(1, metrics.errorDeliveries.get());
        assertEquals(1, metrics.errorMessages.get());
        assertEquals(1, metrics.latencyObservations.get());

        watcher.notify(new Rules());
        callback.doAlarm(Collections.emptyList());
    }

    /**
     * The same AlarmMessage may also list webhook/slack hooks. Those must not be counted
     * as gRPC delivery errors when settings are empty or the hook is foreign.
     */
    @Test
    public void shouldIgnoreForeignHooksWhenRecordingSkippedDelivery() {
        final RecordingGrpcMetrics metrics = new RecordingGrpcMetrics();
        AlarmRulesWatcher watcher = new AlarmRulesWatcher(new Rules(), null, null);
        GRPCCallback callback = new GRPCCallback(watcher, metrics.metrics);

        AlarmMessage message = new AlarmMessage();
        message.setId0("1");
        message.setScope(Scope.Service.name());
        message.setName("mixed hooks");
        message.setAlarmMessage("message");
        message.setRuleName("mock_rule");
        message.setStartTime(System.currentTimeMillis());
        message.getHooks().add(AlarmHooksType.webhook.name() + ".custom");
        message.getHooks().add(AlarmHooksType.gRPC.name() + ".gone");

        assertDoesNotThrow(() -> callback.doAlarm(Collections.singletonList(message)));
        // Only the gRPC.* hook is a skipped gRPC delivery; webhook.* is ignored.
        assertEquals(1, metrics.errorDeliveries.get());
        assertEquals(1, metrics.errorMessages.get());
    }

    /**
     * Clearing every gRPC hook through dynamic config must still count deliveries for
     * messages that name the old hook, so the outage is visible on telemetry.prometheus.
     */
    @Test
    public void shouldRecordErrorWhenAllGrpcSettingsCleared() throws IOException {
        final int port = freePort();
        Rules initial = new Rules();
        GRPCAlarmSetting setting = setting("cleared-hook", true, port);
        initial.getGrpcAlarmSettingMap().put(setting.getFormattedName(), setting);

        final RecordingGrpcMetrics metrics = new RecordingGrpcMetrics();
        AlarmRulesWatcher watcher = new AlarmRulesWatcher(initial, null, null);
        GRPCCallback callback = new GRPCCallback(watcher, metrics.metrics);

        watcher.notify(new Rules());
        assertDoesNotThrow(() -> callback.doAlarm(
            Collections.singletonList(alarmMessage(setting.getFormattedName(), "cleared-settings-alarm"))
        ));
        assertEquals(1, metrics.errorDeliveries.get());
        assertEquals(1, metrics.errorMessages.get());
        assertEquals(1, metrics.latencyObservations.get());
    }

    /**
     * Interrupt while waiting for the peer must finish the attempt as {@code interrupted}:
     * cancel the outbound stream and stop waiting, without restoring the interrupt flag
     * so later hooks on the same AlarmCore thread are not poisoned.
     */
    @Test
    public void shouldRecordInterruptedWhenCallerInterruptedDuringWait() throws Exception {
        final int port = freePort();
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        final Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
            .addService(new StallingAlarmHandler(entered, release))
            .build()
            .start();
        final RecordingGrpcMetrics metrics = new RecordingGrpcMetrics();
        try {
            Rules rules = new Rules();
            GRPCAlarmSetting setting = setting("interrupt-hook", true, port);
            rules.getGrpcAlarmSettingMap().put(setting.getFormattedName(), setting);
            AlarmRulesWatcher watcher = new AlarmRulesWatcher(rules, null, null);
            GRPCCallback callback = new GRPCCallback(watcher, metrics.metrics);

            final Thread caller = new Thread(() ->
                callback.doAlarm(Collections.singletonList(alarmMessage(setting.getFormattedName(), "interrupt-alarm")))
            );
            caller.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(10_000);
            assertTrue(!caller.isAlive(), "caller should finish after interrupt without waiting for the peer");
            release.countDown();

            assertEquals(1, metrics.interruptedDeliveries.get());
            assertEquals(1, metrics.interruptedMessages.get());
            assertEquals(1, metrics.latencyObservations.get());

            watcher.notify(new Rules());
            callback.doAlarm(Collections.emptyList());
        } finally {
            release.countDown();
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A peer that never completes the stream past the client deadline must finish as
     * {@code timeout} (gRPC {@code DEADLINE_EXCEEDED}), same visibility as HTTP's timeout path.
     */
    @Test
    public void shouldRecordTimeoutWhenPeerExceedsDeadline() throws Exception {
        final int port = freePort();
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        final Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
            .addService(new StallingAlarmHandler(entered, release))
            .build()
            .start();
        final RecordingGrpcMetrics metrics = new RecordingGrpcMetrics();
        try {
            Rules rules = new Rules();
            GRPCAlarmSetting setting = setting("timeout-hook", true, port);
            rules.getGrpcAlarmSettingMap().put(setting.getFormattedName(), setting);
            AlarmRulesWatcher watcher = new AlarmRulesWatcher(rules, null, null);
            // 1s deadline mirrors HttpAlarmCallbackTest's injected 1s timeout.
            GRPCCallback callback = new GRPCCallback(watcher, metrics.metrics, 1L);

            callback.doAlarm(Collections.singletonList(alarmMessage(setting.getFormattedName(), "timeout-alarm")));

            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertEquals(1, metrics.timeoutDeliveries.get());
            assertEquals(1, metrics.timeoutMessages.get());
            assertEquals(1, metrics.latencyObservations.get());

            watcher.notify(new Rules());
            callback.doAlarm(Collections.emptyList());
        } finally {
            release.countDown();
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static AlarmGrpcDeliveryMetrics noopMetrics() {
        return new AlarmGrpcDeliveryMetrics(new MetricsCreatorNoop());
    }

    private static GRPCAlarmSetting setting(String name, boolean isDefault, int port) {
        GRPCAlarmSetting setting = new GRPCAlarmSetting(name, AlarmHooksType.gRPC, isDefault);
        setting.setTargetHost("127.0.0.1");
        setting.setTargetPort(port);
        return setting;
    }

    private static AlarmMessage alarmMessage(String hook, String name) {
        AlarmMessage message = new AlarmMessage();
        message.setId0("1");
        message.setId1("2");
        message.setScope(Scope.Service.name());
        message.setName(name);
        message.setAlarmMessage("message");
        message.setRuleName("mock_rule");
        message.setStartTime(System.currentTimeMillis());
        message.setTags(Arrays.asList(new Tag("key", "value")));
        message.getHooks().add(hook);
        return message;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class RecordingGrpcMetrics {
        private final AtomicInteger successAlarmDeliveries = new AtomicInteger();
        private final AtomicInteger successRecoveryDeliveries = new AtomicInteger();
        private final AtomicInteger successAlarmMessages = new AtomicInteger();
        private final AtomicInteger successRecoveryMessages = new AtomicInteger();
        private final AtomicInteger unavailableOrErrorDeliveries = new AtomicInteger();
        private final AtomicInteger unavailableOrErrorMessages = new AtomicInteger();
        private final AtomicInteger unavailableDeliveries = new AtomicInteger();
        private final AtomicInteger unavailableMessages = new AtomicInteger();
        private final AtomicInteger errorDeliveries = new AtomicInteger();
        private final AtomicInteger errorMessages = new AtomicInteger();
        private final AtomicInteger interruptedDeliveries = new AtomicInteger();
        private final AtomicInteger interruptedMessages = new AtomicInteger();
        private final AtomicInteger timeoutDeliveries = new AtomicInteger();
        private final AtomicInteger timeoutMessages = new AtomicInteger();
        private final AtomicInteger latencyObservations = new AtomicInteger();
        private final AtomicReference<Double> lastLatencySeconds = new AtomicReference<>(0.0);
        private final AlarmGrpcDeliveryMetrics metrics;

        private RecordingGrpcMetrics() {
            metrics = new AlarmGrpcDeliveryMetrics(new MetricsCreatorNoop() {
                @Override
                public CounterMetrics createCounter(final String name, final String tips, final MetricsTag.Keys tagKeys,
                                                    final MetricsTag.Values tagValues) {
                    final String kind = tagValues.getValues()[0];
                    final String result = tagValues.getValues().length > 1 ? tagValues.getValues()[1] : "";
                    return new CounterMetrics() {
                        @Override
                        public void inc() {
                            inc(1);
                        }

                        @Override
                        public void inc(final double value) {
                            if ("alarm_grpc_delivery_total".equals(name)) {
                                if (AlarmGrpcDeliveryMetrics.RESULT_SUCCESS.equals(result)) {
                                    if (AlarmGrpcDeliveryMetrics.KIND_ALARM.equals(kind)) {
                                        successAlarmDeliveries.incrementAndGet();
                                    } else if (AlarmGrpcDeliveryMetrics.KIND_RECOVERY.equals(kind)) {
                                        successRecoveryDeliveries.incrementAndGet();
                                    }
                                } else if (AlarmGrpcDeliveryMetrics.RESULT_INTERRUPTED.equals(result)) {
                                    interruptedDeliveries.incrementAndGet();
                                } else if (AlarmGrpcDeliveryMetrics.RESULT_TIMEOUT.equals(result)) {
                                    timeoutDeliveries.incrementAndGet();
                                } else if (AlarmGrpcDeliveryMetrics.RESULT_UNAVAILABLE.equals(result)) {
                                    unavailableDeliveries.incrementAndGet();
                                    unavailableOrErrorDeliveries.incrementAndGet();
                                } else if (AlarmGrpcDeliveryMetrics.RESULT_ERROR.equals(result)) {
                                    errorDeliveries.incrementAndGet();
                                    unavailableOrErrorDeliveries.incrementAndGet();
                                } else {
                                    unavailableOrErrorDeliveries.incrementAndGet();
                                }
                            } else if ("alarm_grpc_delivery_messages_total".equals(name)) {
                                if (AlarmGrpcDeliveryMetrics.RESULT_SUCCESS.equals(result)) {
                                    if (AlarmGrpcDeliveryMetrics.KIND_ALARM.equals(kind)) {
                                        successAlarmMessages.addAndGet((int) value);
                                    } else if (AlarmGrpcDeliveryMetrics.KIND_RECOVERY.equals(kind)) {
                                        successRecoveryMessages.addAndGet((int) value);
                                    }
                                } else if (AlarmGrpcDeliveryMetrics.RESULT_INTERRUPTED.equals(result)) {
                                    interruptedMessages.addAndGet((int) value);
                                } else if (AlarmGrpcDeliveryMetrics.RESULT_TIMEOUT.equals(result)) {
                                    timeoutMessages.addAndGet((int) value);
                                } else if (AlarmGrpcDeliveryMetrics.RESULT_UNAVAILABLE.equals(result)) {
                                    unavailableMessages.addAndGet((int) value);
                                    unavailableOrErrorMessages.addAndGet((int) value);
                                } else if (AlarmGrpcDeliveryMetrics.RESULT_ERROR.equals(result)) {
                                    errorMessages.addAndGet((int) value);
                                    unavailableOrErrorMessages.addAndGet((int) value);
                                } else {
                                    unavailableOrErrorMessages.addAndGet((int) value);
                                }
                            }
                        }
                    };
                }

                @Override
                public HistogramMetrics createHistogramMetric(final String name, final String tips,
                                                             final MetricsTag.Keys tagKeys,
                                                             final MetricsTag.Values tagValues,
                                                             final double... buckets) {
                    return new HistogramMetrics() {
                        @Override
                        public void observe(final double value) {
                            latencyObservations.incrementAndGet();
                            lastLatencySeconds.set(value);
                        }
                    };
                }
            });
        }
    }

    private static final class StallingAlarmHandler extends AlarmServiceGrpc.AlarmServiceImplBase {
        private final CountDownLatch entered;
        private final CountDownLatch release;

        private StallingAlarmHandler(final CountDownLatch entered, final CountDownLatch release) {
            this.entered = entered;
            this.release = release;
        }

        @Override
        public StreamObserver<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage> doAlarm(
            StreamObserver<Response> responseObserver) {
            return new StreamObserver<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage>() {
                @Override
                public void onNext(org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage value) {
                }

                @Override
                public void onError(Throwable throwable) {
                    responseObserver.onError(throwable);
                }

                @Override
                public void onCompleted() {
                    entered.countDown();
                    try {
                        release.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    responseObserver.onNext(Response.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };
        }
    }

    private static final class RecordingAlarmHandler extends AlarmServiceGrpc.AlarmServiceImplBase {
        private final List<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage> alarms =
            new CopyOnWriteArrayList<>();
        private final List<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmRecoveryMessage> recoveries =
            new CopyOnWriteArrayList<>();

        @Override
        public StreamObserver<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage> doAlarm(
            StreamObserver<Response> responseObserver) {
            return new StreamObserver<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage>() {
                @Override
                public void onNext(org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage value) {
                    alarms.add(value);
                }

                @Override
                public void onError(Throwable throwable) {
                    responseObserver.onError(throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(Response.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public StreamObserver<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmRecoveryMessage> doAlarmRecovery(
            StreamObserver<Response> responseObserver) {
            return new StreamObserver<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmRecoveryMessage>() {
                @Override
                public void onNext(org.apache.skywalking.oap.server.core.alarm.grpc.AlarmRecoveryMessage value) {
                    recoveries.add(value);
                }

                @Override
                public void onError(Throwable throwable) {
                    responseObserver.onError(throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(Response.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };
        }
    }

    private void mockAlarmMessage(String hook1, String hook2) {
        AlarmMessage alarmMessage = alarmMessage(hook1, "mock alarm message");
        AlarmMessage alarmMessage2 = alarmMessage(hook1, "mock alarm message2");
        alarmMessage2.setId0("21");
        alarmMessage2.setId1("22");
        alarmMessage2.setRuleName("mock_rule2");
        alarmMessage2.setTags(Arrays.asList(new Tag("key2", "value2")));
        alarmMessageList = Lists.newArrayList(alarmMessage, alarmMessage2);
    }

    private void mockAlarmRecoveryMessage(String hook1, String hook2) {
        AlarmRecoveryMessage alarmRecoveryMessage0 = new AlarmRecoveryMessage(alarmMessageList.get(0));
        AlarmRecoveryMessage alarmRecoveryMessage1 = new AlarmRecoveryMessage(alarmMessageList.get(1));

        alarmRecoveryMessageList = Lists.newArrayList(alarmRecoveryMessage0, alarmRecoveryMessage1);
    }
}
