/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ControlledShutdownResponse;
import org.apache.fluss.rpc.messages.PbStopReplicaReqForBucket;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.ControlledShutdownEvent;
import org.apache.fluss.server.coordinator.event.DeadTabletServerEvent;
import org.apache.fluss.server.coordinator.event.NewTabletServerEvent;
import org.apache.fluss.server.coordinator.statemachine.ReplicaState;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionIneligible;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionStarted;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests replica deletion across controlled shutdown and coordinator recovery. */
class ReplicaDeletionInterleavingTest extends CoordinatorEventProcessorTestBase {

    private final Map<Integer, TabletServerGateway> gateways = new ConcurrentHashMap<>();

    @BeforeEach
    void initializeGateways() {
        for (int serverId = 0; serverId < 3; serverId++) {
            gateways.put(serverId, new TestTabletServerGateway(false, Collections.emptySet()));
        }
        testCoordinatorChannelManager.setGateways(gateways);
    }

    @Test
    void testDeleteFormerLeaderDuringControlledShutdown() throws Exception {
        TablePath tablePath = TablePath.of(defaultDatabase, "drop_during_shutdown");
        long tableId = createTable(tablePath, 0, 1, 2);
        TableBucket bucket = new TableBucket(tableId, 0);
        TableBucketReplica replica = new TableBucketReplica(bucket, 0);
        ServerInfo server = fromContext(ctx -> ctx.getLiveTabletServers().get(0));

        CompletableFuture<ControlledShutdownResponse> shutdownResponse = new CompletableFuture<>();
        eventProcessor
                .getCoordinatorEventManager()
                .put(new ControlledShutdownEvent(0, 0, shutdownResponse));
        shutdownResponse.get(30, TimeUnit.SECONDS);
        verifyContext(
                ctx -> {
                    assertThat(ctx.getBucketLeaderAndIsr(bucket).get().leader()).isNotZero();
                    assertThat(ctx.getReplicaState(replica)).isEqualTo(OnlineReplica);
                    assertThat(ctx.liveTabletServerSet()).doesNotContain(0);
                    assertThat(ctx.liveOrShuttingDownTabletServers()).contains(0);
                });

        // Keep the delete response pending until the server goes down.
        DeferredDeletionGateway shutdownGateway = new DeferredDeletionGateway();
        gateways.put(0, shutdownGateway);
        metadataManager.dropTable(tablePath, false);
        awaitReplicaState(replica, ReplicaDeletionStarted);
        shutdownGateway.deletionRequested.get(30, TimeUnit.SECONDS);

        gateways.remove(0);
        eventProcessor.getCoordinatorEventManager().put(new DeadTabletServerEvent(0));
        awaitReplicaState(replica, ReplicaDeletionIneligible);
        assertThat(zookeeperClient.getTableAssignment(tableId)).isPresent();

        // A fresh gateway cannot complete the old process's pending response. Cleanup must use
        // a new delete request generated by the server-up handler.
        gateways.put(0, new TestTabletServerGateway(false, Collections.emptySet()));
        eventProcessor.getCoordinatorEventManager().put(new NewTabletServerEvent(server));
        awaitAssignmentRemoved(tableId);
    }

    @Test
    void testServerDownBeforeRecoveredDeletionIsAdmitted() throws Exception {
        TablePath blockingTablePath = TablePath.of(defaultDatabase, "blocking_drop");
        long blockingTableId = createTable(blockingTablePath, 1);
        TablePath tablePath = TablePath.of(defaultDatabase, "drop_after_recovery");
        long tableId = createTable(tablePath, 0);
        TableBucketReplica replica = new TableBucketReplica(new TableBucket(tableId, 0), 0);

        eventProcessor.shutdown();
        metadataManager.dropTable(blockingTablePath, false);
        metadataManager.dropTable(tablePath, false);

        DeferredDeletionGateway blockingGateway = new DeferredDeletionGateway();
        gateways.put(1, blockingGateway);
        eventProcessor = buildCoordinatorEventProcessor();
        // Fix the admission order so the recovered target table waits behind another deletion.
        eventProcessor.getLifecycleThrottler().submitTableDropForResume(blockingTableId);
        eventProcessor.startup();
        blockingGateway.deletionRequested.get(30, TimeUnit.SECONDS);
        verifyContext(
                ctx -> {
                    assertThat(ctx.isTableQueuedForDeletion(tableId)).isTrue();
                    assertThat(ctx.getReplicaState(replica)).isEqualTo(OnlineReplica);
                    assertThat(eventProcessor.getLifecycleThrottler().getPendingDropCount())
                            .isEqualTo(1);
                });
        ServerInfo server = fromContext(ctx -> ctx.getLiveTabletServers().get(0));

        gateways.remove(0);
        eventProcessor.getCoordinatorEventManager().put(new DeadTabletServerEvent(0));
        verifyContext(
                ctx -> {
                    assertThat(ctx.liveOrShuttingDownTabletServers()).doesNotContain(0);
                    // Normal server-down handling skips replicas pending deletion, and the
                    // deletion has not yet reached OfflineReplica or ReplicaDeletionStarted.
                    assertThat(ctx.getReplicaState(replica)).isEqualTo(OnlineReplica);
                });

        blockingGateway.allowDeletion.complete(null);
        awaitAssignmentRemoved(blockingTableId);
        awaitReplicaState(replica, ReplicaDeletionIneligible);
        assertThat(zookeeperClient.getTableAssignment(tableId)).isPresent();

        gateways.put(0, new TestTabletServerGateway(false, Collections.emptySet()));
        eventProcessor.getCoordinatorEventManager().put(new NewTabletServerEvent(server));
        awaitAssignmentRemoved(tableId);
    }

    private long createTable(TablePath tablePath, Integer... replicas) throws Exception {
        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(replicas)).build();
        long tableId =
                metadataManager.createTable(
                        tablePath,
                        remoteDataDir,
                        DATA1_TABLE_DESCRIPTOR
                                .withBucketCount(1)
                                .withReplicationFactor(replicas.length),
                        assignment,
                        false);
        retry(
                Duration.ofSeconds(30),
                () ->
                        verifyContext(
                                ctx -> {
                                    assertThat(ctx.replicaCounts(tableId))
                                            .isEqualTo(replicas.length);
                                    assertThat(ctx.areAllReplicasInState(tableId, OnlineReplica))
                                            .isTrue();
                                }));
        return tableId;
    }

    private void awaitReplicaState(TableBucketReplica replica, ReplicaState state) {
        retry(
                Duration.ofSeconds(30),
                () ->
                        verifyContext(
                                ctx -> assertThat(ctx.getReplicaState(replica)).isEqualTo(state)));
    }

    private void awaitAssignmentRemoved(long tableId) {
        retry(
                Duration.ofSeconds(30),
                () -> {
                    assertThat(zookeeperClient.getTableAssignment(tableId)).isEmpty();
                    verifyContext(
                            ctx -> {
                                assertThat(ctx.getAllReplicasForTable(tableId)).isEmpty();
                                assertThat(ctx.isTableQueuedForDeletion(tableId)).isFalse();
                            });
                });
    }

    private void verifyContext(Consumer<CoordinatorContext> assertion) throws Exception {
        fromContext(
                ctx -> {
                    assertion.accept(ctx);
                    return null;
                });
    }

    private <T> T fromContext(Function<CoordinatorContext, T> action) throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(action);
        eventProcessor.getCoordinatorEventManager().put(event);
        try {
            return event.getResultFuture().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AssertionError) {
                throw (AssertionError) e.getCause();
            }
            throw e;
        }
    }

    /** Holds deletion responses so tests can order server-down and deletion admission events. */
    private static class DeferredDeletionGateway extends TestTabletServerGateway {
        private final CompletableFuture<Void> deletionRequested = new CompletableFuture<>();
        private final CompletableFuture<Void> allowDeletion = new CompletableFuture<>();

        private DeferredDeletionGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<StopReplicaResponse> stopReplica(StopReplicaRequest request) {
            CompletableFuture<StopReplicaResponse> response = super.stopReplica(request);
            if (request.getStopReplicasReqsList().stream()
                    .anyMatch(PbStopReplicaReqForBucket::isDelete)) {
                deletionRequested.complete(null);
                return allowDeletion.thenCompose(ignored -> response);
            }
            return response;
        }
    }
}
