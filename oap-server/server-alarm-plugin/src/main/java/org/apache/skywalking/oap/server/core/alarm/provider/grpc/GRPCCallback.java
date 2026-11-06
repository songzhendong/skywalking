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

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.alarm.AlarmCallback;
import org.apache.skywalking.oap.server.core.alarm.AlarmGrpcDeliveryMetrics;
import org.apache.skywalking.oap.server.core.alarm.AlarmMessage;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecoveryMessage;
import org.apache.skywalking.oap.server.core.alarm.grpc.AlarmServiceGrpc;
import org.apache.skywalking.oap.server.core.alarm.grpc.AlarmTags;
import org.apache.skywalking.oap.server.core.alarm.grpc.KeyStringValuePair;
import org.apache.skywalking.oap.server.core.alarm.grpc.Response;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.alarm.provider.AlarmHooksType;
import org.apache.skywalking.oap.server.core.alarm.provider.AlarmModuleConfig;
import org.apache.skywalking.oap.server.core.alarm.provider.AlarmRulesWatcher;
import org.apache.skywalking.oap.server.library.client.grpc.GRPCClient;
import org.apache.skywalking.oap.server.library.util.GRPCStreamStatus;

/**
 * Use SkyWalking alarm grpc API call a remote methods.
 */
@Slf4j
public class GRPCCallback implements AlarmCallback {

    private final AlarmRulesWatcher alarmRulesWatcher;

    private final AlarmGrpcDeliveryMetrics deliveryMetrics;

    private final long deadlineSeconds;

    private final Map<String, AlarmServiceGrpc.AlarmServiceStub> alarmServiceStubMap;

    private final Map<String, GRPCClient> grpcClientMap;

    /**
     * Uses {@link AlarmModuleConfig#DEFAULT_GRPC_TIMEOUT_SECONDS}. Prefer the three-arg constructor
     * when wiring from {@code alarm.default.grpcTimeout}.
     */
    public GRPCCallback(final AlarmRulesWatcher alarmRulesWatcher,
                        final AlarmGrpcDeliveryMetrics deliveryMetrics) {
        this(alarmRulesWatcher, deliveryMetrics, AlarmModuleConfig.DEFAULT_GRPC_TIMEOUT_SECONDS);
    }

    /**
     * @param deadlineSeconds gRPC call deadline for each alarm / recovery stream (seconds). Values
     *                        below 1 are raised to 1, same floor as a short test injection.
     */
    public GRPCCallback(final AlarmRulesWatcher alarmRulesWatcher,
                        final AlarmGrpcDeliveryMetrics deliveryMetrics,
                        final long deadlineSeconds) {
        this.alarmRulesWatcher = alarmRulesWatcher;
        this.deliveryMetrics = Objects.requireNonNull(deliveryMetrics, "deliveryMetrics");
        this.deadlineSeconds = Math.max(1L, deadlineSeconds);
        this.alarmServiceStubMap = new HashMap<>();
        this.grpcClientMap = new HashMap<>();
        onGRPCAlarmSettingUpdated(alarmRulesWatcher.getGrpchookSetting());
    }

    @Override
    public void doAlarm(List<AlarmMessage> alarmMessages) {
        doAlarmCallback(alarmMessages, false);
    }

    @Override
    public void doAlarmRecovery(List<AlarmMessage> alarmRecoveryMessages) {
        doAlarmCallback(alarmRecoveryMessages, true);
    }

    private void doAlarmCallback(List<AlarmMessage> alarmMessages, boolean isRecovery) {
        // recreate gRPC client and stub if host and port configuration changed.
        Map<String, GRPCAlarmSetting> settinsMap = alarmRulesWatcher.getGrpchookSetting();
        onGRPCAlarmSettingUpdated(settinsMap);

        final Map<String, List<AlarmMessage>> groupedMessages = groupMessagesByHook(alarmMessages);
        if (settinsMap == null || settinsMap.isEmpty()) {
            // Only gRPC.* hooks still need a failure series when every gRPC target is gone.
            // Foreign hooks (webhook.*, slack.*, …) share the same AlarmMessage.hooks set.
            groupedMessages.forEach((hook, messages) -> {
                if (isGrpcHook(hook)) {
                    recordSkippedDelivery(messages, isRecovery);
                }
            });
            return;
        }

        groupedMessages.forEach((hook, messages) -> {
            if (!isGrpcHook(hook)) {
                return;
            }
            final AlarmServiceGrpc.AlarmServiceStub stub = alarmServiceStubMap.get(hook);
            final GRPCAlarmSetting setting = settinsMap.get(hook);
            if (stub == null || setting == null) {
                // Same as a WeLink token outage: the attempt never left this process, but must still count.
                recordSkippedDelivery(messages, isRecovery);
                return;
            }
            if (!isRecovery) {
                sendAlarmMessages(stub, messages, setting);
            } else {
                sendAlarmRecoveryMessages(stub, messages, setting);
            }
        });

    }

    private static boolean isGrpcHook(final String hookName) {
        return hookName != null && hookName.startsWith(AlarmHooksType.gRPC.name() + Const.POINT);
    }

    private void sendAlarmMessages(AlarmServiceGrpc.AlarmServiceStub alarmServiceStub,
                                   List<AlarmMessage> alarmMessages,
                                   GRPCAlarmSetting alarmSetting) {
        final long startedNanos = System.nanoTime();
        final GRPCStreamStatus status = new GRPCStreamStatus();
        final AtomicReference<Throwable> streamError = new AtomicReference<>();
        boolean interrupted = false;
        try {
            StreamObserver<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage> streamObserver =
                    alarmServiceStub.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS).doAlarm(new StreamObserver<Response>() {
                        @Override
                        public void onNext(Response response) {
                            // ignore empty response
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            streamError.set(throwable);
                            status.done();
                            log.warn("Send alarm message failed: {}", throwable.getMessage());
                        }

                        @Override
                        public void onCompleted() {
                            status.done();
                            if (log.isDebugEnabled()) {
                                log.debug("Send alarm message successful.");
                            }
                        }
                    });

            alarmMessages.forEach(message -> {
                org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage.Builder builder =
                        org.apache.skywalking.oap.server.core.alarm.grpc.AlarmMessage.newBuilder();

                builder.setScopeId(message.getScopeId());
                builder.setScope(message.getScope());
                builder.setName(message.getName());
                builder.setId0(message.getId0());
                builder.setId1(message.getId1());
                builder.setRuleName(message.getRuleName());
                builder.setAlarmMessage(message.getAlarmMessage());
                builder.setStartTime(message.getStartTime());
                builder.setUuid(message.getUuid());
                AlarmTags.Builder alarmTagsBuilder = AlarmTags.newBuilder();
                message.getTags().forEach(m -> alarmTagsBuilder.addData(KeyStringValuePair.newBuilder().setKey(m.getKey()).setValue(m.getValue()).build()));
                builder.setTags(alarmTagsBuilder.build());
                streamObserver.onNext(builder.build());
            });

            streamObserver.onCompleted();

            interrupted = waitUntilDone(status, alarmMessages.size(), alarmSetting, false);
        } catch (Throwable throwable) {
            streamError.compareAndSet(null, throwable);
            status.done();
            log.warn("Send alarm message failed before the peer replied: {}", throwable.getMessage());
        } finally {
            recordDelivery(AlarmGrpcDeliveryMetrics.KIND_ALARM, alarmMessages.size(), startedNanos, interrupted, streamError.get());
        }
    }

    private void sendAlarmRecoveryMessages(AlarmServiceGrpc.AlarmServiceStub alarmServiceStub,
                                           List<AlarmMessage> alarmMessages,
                                           GRPCAlarmSetting alarmSetting) {
        final long startedNanos = System.nanoTime();
        final GRPCStreamStatus status = new GRPCStreamStatus();
        final AtomicReference<Throwable> streamError = new AtomicReference<>();
        boolean interrupted = false;
        try {
            StreamObserver<org.apache.skywalking.oap.server.core.alarm.grpc.AlarmRecoveryMessage> streamObserver =
                    alarmServiceStub.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS).doAlarmRecovery(new StreamObserver<Response>() {
                        @Override
                        public void onNext(Response response) {
                            // ignore empty response
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            streamError.set(throwable);
                            status.done();
                            log.warn("Send alarm recovery message failed: {}", throwable.getMessage());
                        }

                        @Override
                        public void onCompleted() {
                            status.done();
                            if (log.isDebugEnabled()) {
                                log.debug("Send alarm recovery message successful.");
                            }
                        }
                    });

            alarmMessages.forEach(message -> {
                org.apache.skywalking.oap.server.core.alarm.grpc.AlarmRecoveryMessage.Builder builder =
                        org.apache.skywalking.oap.server.core.alarm.grpc.AlarmRecoveryMessage.newBuilder();
                AlarmRecoveryMessage recoveryMessage = (AlarmRecoveryMessage) message;
                builder.setScopeId(recoveryMessage.getScopeId());
                builder.setScope(recoveryMessage.getScope());
                builder.setName(recoveryMessage.getName());
                builder.setId0(recoveryMessage.getId0());
                builder.setId1(recoveryMessage.getId1());
                builder.setRuleName(recoveryMessage.getRuleName());
                builder.setAlarmMessage(recoveryMessage.getAlarmMessage());
                builder.setStartTime(recoveryMessage.getStartTime());
                builder.setUuid(recoveryMessage.getUuid());
                builder.setRecoveryTime(recoveryMessage.getRecoveryTime());
                AlarmTags.Builder alarmTagsBuilder = AlarmTags.newBuilder();
                message.getTags().forEach(m -> alarmTagsBuilder.addData(KeyStringValuePair.newBuilder().setKey(m.getKey()).setValue(m.getValue()).build()));
                builder.setTags(alarmTagsBuilder.build());
                streamObserver.onNext(builder.build());
            });

            streamObserver.onCompleted();

            interrupted = waitUntilDone(status, alarmMessages.size(), alarmSetting, true);
        } catch (Throwable throwable) {
            streamError.compareAndSet(null, throwable);
            status.done();
            log.warn("Send alarm recovery message failed before the peer replied: {}", throwable.getMessage());
        } finally {
            recordDelivery(AlarmGrpcDeliveryMetrics.KIND_RECOVERY, alarmMessages.size(), startedNanos, interrupted, streamError.get());
        }
    }

    private void recordSkippedDelivery(final List<AlarmMessage> alarmMessages, final boolean isRecovery) {
        deliveryMetrics.record(
            isRecovery ? AlarmGrpcDeliveryMetrics.KIND_RECOVERY : AlarmGrpcDeliveryMetrics.KIND_ALARM,
            AlarmGrpcDeliveryMetrics.RESULT_ERROR,
            alarmMessages.size(),
            0
        );
    }

    private boolean waitUntilDone(final GRPCStreamStatus status,
                                  final int messageCount,
                                  final GRPCAlarmSetting alarmSetting,
                                  final boolean recovery) {
        long sleepTime = 0;
        long cycle = 100L;
        boolean interrupted = false;

        // For memory safe of oap, we must wait for the peer confirmation.
        while (!status.isDone()) {
            try {
                sleepTime += cycle;
                Thread.sleep(cycle);
            } catch (InterruptedException ignored) {
                interrupted = true;
            }

            if (log.isDebugEnabled()) {
                if (recovery) {
                    log.debug("Send {} alarm recovery message to {}:{}.", messageCount,
                            alarmSetting.getTargetHost(), alarmSetting.getTargetPort()
                    );
                } else {
                    log.debug("Send {} alarm message to {}:{}.", messageCount,
                            alarmSetting.getTargetHost(), alarmSetting.getTargetPort()
                    );
                }
            }

            if (sleepTime > 2000L) {
                if (recovery) {
                    log.warn("Send {} alarm recovery message to {}:{}, wait {} milliseconds.", messageCount,
                            alarmSetting.getTargetHost(), alarmSetting.getTargetPort(), sleepTime
                    );
                } else {
                    log.warn("Send {} alarm message to {}:{}, wait {} milliseconds.", messageCount,
                            alarmSetting.getTargetHost(), alarmSetting.getTargetPort(), sleepTime
                    );
                }
                cycle = 2000L;
            }
        }
        return interrupted;
    }

    private void recordDelivery(final String messageKind,
                                final int messageCount,
                                final long startedNanos,
                                final boolean interrupted,
                                final Throwable streamError) {
        deliveryMetrics.record(
            messageKind,
            classifyDeliveryResult(interrupted, streamError),
            messageCount,
            (System.nanoTime() - startedNanos) / 1.0E9D
        );
    }

    /**
     * When the wait loop was interrupted, that outcome wins even if the stream also closed with
     * {@code DEADLINE_EXCEEDED} or another status — same idea as HTTP recording {@code interrupted}
     * from {@code InterruptedException} rather than racing another failure class.
     */
    static String classifyDeliveryResult(final boolean interrupted, final Throwable streamError) {
        if (interrupted) {
            return AlarmGrpcDeliveryMetrics.RESULT_INTERRUPTED;
        }
        if (streamError != null) {
            return AlarmGrpcDeliveryMetrics.resultOf(streamError);
        }
        return AlarmGrpcDeliveryMetrics.RESULT_SUCCESS;
    }

    private void onGRPCAlarmSettingUpdated(Map<String, GRPCAlarmSetting> newAlarmSettingMap) {
        if (newAlarmSettingMap == null || newAlarmSettingMap.isEmpty()) {
            // Keep maps non-null so a later non-empty dynamic config update can rebuild
            // clients without NPE on grpcClientMap / alarmServiceStubMap access.
            grpcClientMap.forEach((name, grpcClient) -> {
                grpcClient.shutdown();
                log.debug("gRPC alarm hook target is empty, shutdown the old gRPC client.");
            });
            grpcClientMap.clear();
            alarmServiceStubMap.clear();
            return;
        }

        // Drop clients for hooks that disappeared from the new config (e.g. {A,B} -> {A}).
        for (String name : new ArrayList<>(grpcClientMap.keySet())) {
            if (!newAlarmSettingMap.containsKey(name)) {
                shutdownClient(name, "removed");
            }
        }

        newAlarmSettingMap.forEach((name, newAlarmSetting) -> {
            if (newAlarmSetting == null || newAlarmSetting.isEmptySetting()) {
                shutdownClient(name, "empty");
                return;
            }
            final GRPCClient existing = grpcClientMap.get(name);
            if (sameTarget(existing, newAlarmSetting)) {
                return;
            }
            final GRPCClient grpcClient = new GRPCClient(newAlarmSetting.getTargetHost(), newAlarmSetting.getTargetPort());
            grpcClient.connect();
            final GRPCClient previous = grpcClientMap.put(name, grpcClient);
            alarmServiceStubMap.put(name, AlarmServiceGrpc.newStub(grpcClient.getChannel()));
            if (previous != null) {
                previous.shutdown();
                log.debug("gRPC alarm hook target is changed, shutdown the old gRPC client.");
            }
        });
    }

    private static boolean sameTarget(GRPCClient existing, GRPCAlarmSetting setting) {
        return existing != null
            && Objects.equals(existing.getHost(), setting.getTargetHost())
            && existing.getPort() == setting.getTargetPort();
    }

    private void shutdownClient(String name, String reason) {
        GRPCClient grpcClient = grpcClientMap.remove(name);
        alarmServiceStubMap.remove(name);
        if (grpcClient != null) {
            grpcClient.shutdown();
            log.debug("gRPC alarm hook target is {}, shutdown the old gRPC client.", reason);
        }
    }
}
