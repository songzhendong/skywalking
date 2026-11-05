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

package org.apache.skywalking.oap.server.fetcher.cilium.nodes;

import io.cilium.api.observer.ObserverGrpc;
import lombok.Getter;
import org.apache.skywalking.oap.server.core.cluster.RemoteInstance;
import org.apache.skywalking.oap.server.core.remote.client.Address;
import org.apache.skywalking.oap.server.fetcher.cilium.CiliumFetcherConfig;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.skywalking.oap.server.testing.util.ReflectUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class CiliumNodeManagerTest {

    @Mock
    private ModuleManager moduleManager;
    private CiliumNodeManager ciliumNodeManager;
    private NodeUpdateListener nodeUpdateListener;

    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {
                "none-node-cluster-with-cilium-nodes",
                Collections.emptyList(),
                Arrays.asList(ciliumNode("c1")),
                Collections.emptyList(),
            },
            {
                "single-node-cluster-with-single-nodes",
                Arrays.asList(oapInstance("a1", true)),
                Arrays.asList(ciliumNode("c1")),
                Arrays.asList(ciliumNode("c1"))
            },
            {
                "single-node-cluster-with-multiple-nodes",
                Arrays.asList(oapInstance("a1", true)),
                Arrays.asList(ciliumNode("c1"), ciliumNode("c2")),
                Arrays.asList(ciliumNode("c1"), ciliumNode("c2"))
            },
            {
                "multiple-node-cluster-with-single-nodes",
                Arrays.asList(oapInstance("a1", true), oapInstance("a2", false)),
                Arrays.asList(ciliumNode("c1")),
                Arrays.asList(ciliumNode("c1"))
            },
            {
                "multiple-node-cluster-with-single-nodes-2",
                Arrays.asList(oapInstance("a1", false), oapInstance("a2", true)),
                Arrays.asList(ciliumNode("c1")),
                Collections.emptyList()
            },
            {
                "multiple-node-cluster-with-multiple-nodes",
                Arrays.asList(oapInstance("a1", true), oapInstance("a2", false)),
                Arrays.asList(ciliumNode("c1"), ciliumNode("c2")),
                Arrays.asList(ciliumNode("c1"))
            },
            {
                "multiple-node-cluster-with-multiple-nodes-2",
                Arrays.asList(oapInstance("a1", true), oapInstance("a2", false)),
                Arrays.asList(ciliumNode("c1"), ciliumNode("c2"), ciliumNode("c3")),
                Arrays.asList(ciliumNode("c1"))
            },
            {
                "multiple-node-cluster-with-multiple-nodes-3",
                Arrays.asList(oapInstance("a1", false), oapInstance("a2", true)),
                Arrays.asList(ciliumNode("c1"), ciliumNode("c2"), ciliumNode("c3")),
                Arrays.asList(ciliumNode("c2"), ciliumNode("c3"))
            },
        });
    }

    @BeforeEach
    public void prepare() {
        ciliumNodeManager = new CiliumNodeManager(moduleManager, new NoopClientBuilder(), new CiliumFetcherConfig());
        nodeUpdateListener = new NodeUpdateListener();
        ciliumNodeManager.addListener(nodeUpdateListener);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("data")
    public void test(String name,
                     List<RemoteInstance> allOAPInstances,
                     List<CiliumNode> allCiliumNodes,
                     List<CiliumNode> shouldMonitorNodeBySelf) {
        ReflectUtil.setInternalState(ciliumNodeManager, "remoteInstances", allOAPInstances);
        ReflectUtil.setInternalState(ciliumNodeManager, "allNodes", allCiliumNodes);
        ciliumNodeManager.refreshUsingNodes();
        final List<CiliumNode> nodes = nodeUpdateListener.getNodes();
        nodes.sort(Comparator.comparing(CiliumNode::getAddress));
        assertThat(nodes).isEqualTo(shouldMonitorNodeBySelf);
    }

    /**
     * Share shrink Closes c2 while self stays present. allNodes must keep address c2 as a
     * fresh open instance (not the closed object); when the share grows back, Create can
     * open a non-null Observer stub again.
     */
    @Test
    public void shouldReplaceClosedPeerInAllNodesWhenShareMovesAwayAndBack() {
        final ObserverGrpc.ObserverBlockingStub stub = mock(ObserverGrpc.ObserverBlockingStub.class);
        final AtomicInteger builds = new AtomicInteger();
        final ClientBuilder builder = new ClientBuilder() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T buildClient(String host, int port, Class<T> stubClass) {
                if (ObserverGrpc.ObserverBlockingStub.class.equals(stubClass)) {
                    builds.incrementAndGet();
                    return (T) stub;
                }
                return null;
            }
        };
        ciliumNodeManager = new CiliumNodeManager(moduleManager, builder, new CiliumFetcherConfig());
        nodeUpdateListener = new NodeUpdateListener();
        ciliumNodeManager.addListener(nodeUpdateListener);

        final CiliumNode c1 = new CiliumNode("c1", builder);
        final CiliumNode c2 = new CiliumNode("c2", builder);
        ReflectUtil.setInternalState(ciliumNodeManager, "remoteInstances",
            Arrays.asList(oapInstance("a1", true)));
        ReflectUtil.setInternalState(ciliumNodeManager, "allNodes",
            new ArrayList<>(Arrays.asList(c1, c2)));
        ciliumNodeManager.refreshUsingNodes();
        assertThat(nodeUpdateListener.getNodes()).containsExactlyInAnyOrder(c1, c2);
        assertThat(c2.getObserverStub()).isSameAs(stub);

        // Second backend takes the other half; c2 leaves using-nodes and is Closed.
        ciliumNodeManager.onClusterNodesChanged(
            Arrays.asList(oapInstance("a1", true), oapInstance("a2", false)));
        assertThat(nodeUpdateListener.getNodes()).containsExactly(c1);
        assertThat(c2.isClosed()).isTrue();
        assertNull(c2.getObserverStub());

        final List<CiliumNode> allAfterClose =
            ReflectUtil.getInternalState(ciliumNodeManager, "allNodes");
        assertThat(allAfterClose).hasSize(2);
        final CiliumNode replacedC2 = allAfterClose.stream()
            .filter(n -> "c2".equals(n.getAddress()))
            .findFirst()
            .orElseThrow(AssertionError::new);
        assertThat(replacedC2).isNotSameAs(c2);
        assertThat(replacedC2.isClosed()).isFalse();
        // Closed instance must not remain in the peer inventory (identity, not address equals).
        assertThat(allAfterClose.stream().noneMatch(n -> n == c2)).isTrue();

        // Share grows back: Create must use the fresh inventory node, not the closed one.
        final int buildsBeforeReassign = builds.get();
        ciliumNodeManager.onClusterNodesChanged(Arrays.asList(oapInstance("a1", true)));
        final List<CiliumNode> usingNodes =
            ReflectUtil.getInternalState(ciliumNodeManager, "usingNodes");
        assertThat(usingNodes).hasSize(2);
        final CiliumNode reassignedC2 = usingNodes.stream()
            .filter(n -> "c2".equals(n.getAddress()))
            .findFirst()
            .orElseThrow(AssertionError::new);
        assertThat(reassignedC2).isSameAs(replacedC2);
        assertThat(reassignedC2).isNotSameAs(c2);
        assertThat(reassignedC2.isClosed()).isFalse();
        assertThat(reassignedC2.getObserverStub()).isSameAs(stub);
        assertThat(builds.get()).isGreaterThan(buildsBeforeReassign);
        assertThat(nodeUpdateListener.getNodes()).containsExactlyInAnyOrderElementsOf(usingNodes);
    }

    /**
     * Empty cluster Closes every using-node; inventory must still hold fresh open peers so
     * a later self assignment can open stubs.
     */
    @Test
    public void shouldAssignFreshNodesAfterClusterClearsAndSelfReturns() {
        final ObserverGrpc.ObserverBlockingStub stub = mock(ObserverGrpc.ObserverBlockingStub.class);
        final ClientBuilder builder = new ClientBuilder() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T buildClient(String host, int port, Class<T> stubClass) {
                if (ObserverGrpc.ObserverBlockingStub.class.equals(stubClass)) {
                    return (T) stub;
                }
                return null;
            }
        };
        ciliumNodeManager = new CiliumNodeManager(moduleManager, builder, new CiliumFetcherConfig());
        nodeUpdateListener = new NodeUpdateListener();
        ciliumNodeManager.addListener(nodeUpdateListener);

        final CiliumNode original = new CiliumNode("c1", builder);
        ReflectUtil.setInternalState(ciliumNodeManager, "remoteInstances",
            Arrays.asList(oapInstance("a1", true)));
        ReflectUtil.setInternalState(ciliumNodeManager, "allNodes",
            new ArrayList<>(Collections.singletonList(original)));
        ciliumNodeManager.refreshUsingNodes();
        assertThat(nodeUpdateListener.getNodes()).containsExactly(original);

        ciliumNodeManager.onClusterNodesChanged(Collections.emptyList());
        assertThat(nodeUpdateListener.getNodes()).isEmpty();
        assertThat(original.isClosed()).isTrue();
        assertNull(original.getObserverStub());

        final List<CiliumNode> allAfterClose =
            ReflectUtil.getInternalState(ciliumNodeManager, "allNodes");
        assertThat(allAfterClose).hasSize(1);
        assertThat(allAfterClose.get(0)).isNotSameAs(original);
        assertThat(allAfterClose.get(0).isClosed()).isFalse();

        ciliumNodeManager.onClusterNodesChanged(Arrays.asList(oapInstance("a1", true)));
        final List<CiliumNode> usingNodes =
            ReflectUtil.getInternalState(ciliumNodeManager, "usingNodes");
        assertThat(usingNodes).hasSize(1);
        assertThat(usingNodes.get(0).getAddress()).isEqualTo("c1");
        assertThat(usingNodes.get(0).isClosed()).isFalse();
        assertThat(usingNodes.get(0)).isNotSameAs(original);
        assertThat(usingNodes.get(0).getObserverStub()).isSameAs(stub);
        assertThat(nodeUpdateListener.getNodes()).containsExactly(usingNodes.get(0));
    }

    /**
     * With one monitor covering notify, a blocking Create holds {@code nodeLock} so a
     * concurrent Close waits and events stay ordered.
     */
    @Test
    public void shouldNotifyCloseAfterCreateUnderSingleLock() throws Exception {
        final List<String> order = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch createStarted = new CountDownLatch(1);
        final CountDownLatch allowCreateFinish = new CountDownLatch(1);
        ReflectUtil.setInternalState(ciliumNodeManager, "listeners", new CopyOnWriteArrayList<>(
            Collections.singletonList(new CiliumNodeUpdateListener() {
                @Override
                public void onNodeAdded(CiliumNode node) {
                    order.add("create:" + node.getAddress());
                    createStarted.countDown();
                    try {
                        assertThat(allowCreateFinish.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void onNodeDelete(CiliumNode node) {
                    order.add("close:" + node.getAddress());
                }
            })));

        ReflectUtil.setInternalState(ciliumNodeManager, "remoteInstances",
            Arrays.asList(oapInstance("a1", true)));
        ReflectUtil.setInternalState(ciliumNodeManager, "allNodes",
            new ArrayList<>(Collections.singletonList(ciliumNode("c1"))));

        final Thread assigner = new Thread(ciliumNodeManager::refreshUsingNodes, "cilium-assign");
        assigner.start();
        assertThat(createStarted.await(5, TimeUnit.SECONDS)).isTrue();

        final Thread closer = new Thread(
            () -> ciliumNodeManager.onClusterNodesChanged(Collections.emptyList()), "cilium-close");
        closer.start();
        // Closer blocks on nodeLock until Create finishes notify under that lock.
        Thread.sleep(200);
        allowCreateFinish.countDown();
        assigner.join(5000);
        closer.join(5000);

        assertThat(order).containsExactly("create:c1", "close:c1");
    }

    /**
     * Peer add/remove, cluster watcher and refresh share {@code allNodes}; without one lock
     * this used to throw {@link java.util.ConcurrentModificationException} or mis-shard.
     */
    @Test
    public void shouldTolerateConcurrentNodeAndClusterUpdates() throws Exception {
        ReflectUtil.setInternalState(ciliumNodeManager, "remoteInstances",
            Arrays.asList(oapInstance("a1", true), oapInstance("a2", false)));
        ReflectUtil.setInternalState(ciliumNodeManager, "allNodes", new ArrayList<>());

        final int rounds = 200;
        final ExecutorService pool = Executors.newFixedThreadPool(3);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        pool.execute(() -> runUntilDone(start, failure, rounds, i -> {
            ReflectUtil.invokeMethod(ciliumNodeManager, "addOrUpdateNode", ciliumNode("c" + (i % 8)));
        }));
        pool.execute(() -> runUntilDone(start, failure, rounds, i -> {
            ReflectUtil.invokeMethod(ciliumNodeManager, "removeNode", ciliumNode("c" + ((i + 3) % 8)));
        }));
        pool.execute(() -> runUntilDone(start, failure, rounds, i -> {
            if (i % 2 == 0) {
                ciliumNodeManager.onClusterNodesChanged(
                    Arrays.asList(oapInstance("a1", true), oapInstance("a2", false)));
            } else {
                ciliumNodeManager.onClusterNodesChanged(Collections.emptyList());
            }
            ciliumNodeManager.refreshUsingNodes();
        }));

        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        assertNull(failure.get(), () -> "concurrent update failed: " + failure.get());

        final List<CiliumNode> usingNodes =
            ReflectUtil.getInternalState(ciliumNodeManager, "usingNodes");
        assertThat(usingNodes).allMatch(node -> !node.isClosed());
        assertThat(nodeUpdateListener.getNodes())
            .containsExactlyInAnyOrderElementsOf(usingNodes);

        final List<CiliumNode> allNodes =
            ReflectUtil.getInternalState(ciliumNodeManager, "allNodes");
        assertThat(allNodes).allMatch(node -> !node.isClosed());
    }

    private static void runUntilDone(CountDownLatch start,
                                     AtomicReference<Throwable> failure,
                                     int rounds,
                                     RoundAction action) {
        try {
            start.await();
            for (int i = 0; i < rounds; i++) {
                if (failure.get() != null) {
                    return;
                }
                action.run(i);
            }
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    @FunctionalInterface
    private interface RoundAction {
        void run(int round) throws Exception;
    }

    private static CiliumNode ciliumNode(String address) {
        return new CiliumNode(address, null);
    }

    private static RemoteInstance oapInstance(String address, boolean self) {
        return new RemoteInstance(new Address(address, 0, self));
    }

    @Getter
    private static class NodeUpdateListener implements CiliumNodeUpdateListener {
        private List<CiliumNode> nodes;

        public NodeUpdateListener() {
            this.nodes = new ArrayList<>();
        }

        @Override
        public void onNodeAdded(CiliumNode node) {
            this.nodes.add(node);
        }

        @Override
        public void onNodeDelete(CiliumNode node) {
            this.nodes.remove(node);
        }
    }

    private static class NoopClientBuilder implements ClientBuilder {

        @Override
        public <T> T buildClient(String host, int port, Class<T> stubClass) {
            return null;
        }
    }
}
