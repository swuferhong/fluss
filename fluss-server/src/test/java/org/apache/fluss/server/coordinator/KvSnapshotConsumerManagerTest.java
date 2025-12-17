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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.ConsumeKvSnapshotForBucket;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.KvSnapshotConsumer;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.createServers;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvSnapshotConsumerManager}. */
public class KvSnapshotConsumerManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final TablePath PARTITION_TABLE_PATH =
            new TablePath("test_db_1", "test_partition_table");
    private static final long PARTITION_TABLE_ID = 150008L;
    private static final TableInfo PARTITION_TABLE_INFO =
            TableInfo.of(
                    PARTITION_TABLE_PATH,
                    PARTITION_TABLE_ID,
                    1,
                    DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                    System.currentTimeMillis(),
                    System.currentTimeMillis());
    private static final long PARTITION_ID_1 = 19001L;
    private static final PhysicalTablePath PARTITION_TABLE_PATH_1 =
            PhysicalTablePath.of(PARTITION_TABLE_PATH, "2024");

    private static final long PARTITION_ID_2 = 19002L;
    private static final PhysicalTablePath PARTITION_TABLE_PATH_2 =
            PhysicalTablePath.of(PARTITION_TABLE_PATH, "2025");

    private static final int NUM_BUCKETS = DATA1_TABLE_INFO_PK.getNumBuckets();
    private static final TableBucket t0b0 = new TableBucket(DATA1_TABLE_ID_PK, 0);
    private static final TableBucket t0b1 = new TableBucket(DATA1_TABLE_ID_PK, 1);
    private static final TableBucket t1p0b0 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_1, 0);
    private static final TableBucket t1p0b1 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_1, 1);
    private static final TableBucket t1p1b0 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_2, 0);

    protected static ZooKeeperClient zookeeperClient;

    private CoordinatorContext coordinatorContext;
    private ManualClock manualClock;
    private ManuallyTriggeredScheduledExecutorService clearConsumerScheduler;
    private KvSnapshotConsumerManager kvSnapshotConsumerManager;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void beforeEach() throws Exception {
        initCoordinatorContext();
        Configuration conf = new Configuration();
        // set a huge expiration check interval to avoid expiration check.
        conf.set(ConfigOptions.KV_SNAPSHOT_CONSUMER_EXPIRATION_CHECK_INTERVAL, Duration.ofDays(7));
        manualClock = new ManualClock(System.currentTimeMillis());
        clearConsumerScheduler = new ManuallyTriggeredScheduledExecutorService();
        kvSnapshotConsumerManager =
                new KvSnapshotConsumerManager(
                        conf,
                        zookeeperClient,
                        coordinatorContext,
                        clearConsumerScheduler,
                        manualClock,
                        TestingMetricGroups.COORDINATOR_METRICS);
        kvSnapshotConsumerManager.start();
        initialZookeeper();
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    private static void initialZookeeper() throws Exception {
        List<TableBucket> tableBuckets = Arrays.asList(t0b0, t0b1, t1p0b0, t1p0b1, t1p1b0);
        for (TableBucket tb : tableBuckets) {
            zookeeperClient.registerTableBucketSnapshot(
                    tb, new BucketSnapshot(0L, 0L, "test-path"));
        }
    }

    @Test
    void testInitialize() throws Exception {
        assertThat(
                        snapshotConsumerNotExists(
                                Arrays.asList(
                                        new ConsumeKvSnapshotForBucket(t0b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t0b1, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b1, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p1b0, 0L))))
                .isTrue();
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(0);

        // test initialize from zookeeper when coordinator is started.
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(1000L);
        register(consumer, new ConsumeKvSnapshotForBucket(t0b0, 0L));
        register(consumer, new ConsumeKvSnapshotForBucket(t0b1, 0L));
        register(consumer, new ConsumeKvSnapshotForBucket(t1p0b0, 0L));
        register(consumer, new ConsumeKvSnapshotForBucket(t1p0b1, 0L));
        register(consumer, new ConsumeKvSnapshotForBucket(t1p1b0, 0L));
        zookeeperClient.registerKvSnapshotConsumer("consumer1", consumer);

        kvSnapshotConsumerManager.initialize();

        assertThat(
                        snapshotConsumerExists(
                                Arrays.asList(
                                        new ConsumeKvSnapshotForBucket(t0b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t0b1, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b1, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p1b0, 0L))))
                .isTrue();
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(5);

        // check detail content.
        Map<Long, Long[]> tableIdToSnapshots = new HashMap<>();
        Map<Long, Set<Long>> tableIdToPartitions = new HashMap<>();
        Map<Long, Long[]> partitionIdToSnapshots = new HashMap<>();
        tableIdToSnapshots.put(DATA1_TABLE_ID_PK, new Long[] {0L, 0L, -1L});
        tableIdToPartitions.put(
                PARTITION_TABLE_ID, new HashSet<>(Arrays.asList(PARTITION_ID_1, PARTITION_ID_2)));
        partitionIdToSnapshots.put(PARTITION_ID_1, new Long[] {0L, 0L, -1L});
        partitionIdToSnapshots.put(PARTITION_ID_2, new Long[] {0L, -1L, -1L});
        KvSnapshotConsumer expectedConsumer =
                new KvSnapshotConsumer(
                        1000L, tableIdToSnapshots, tableIdToPartitions, partitionIdToSnapshots);
        assertThat(kvSnapshotConsumerManager.getKvSnapshotConsumer("consumer1"))
                .isEqualTo(expectedConsumer);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer1")).hasValue(expectedConsumer);
    }

    @Test
    void testRegisterAndUnregister() throws Exception {
        Map<Long, List<ConsumeKvSnapshotForBucket>> tableIdToRegisterBucket = initRegisterBuckets();
        register("consumer1", tableIdToRegisterBucket);

        // first register snapshot to zk.
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b0, new BucketSnapshot(1L, 10L, "test-path"));
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b1, new BucketSnapshot(1L, 10L, "test-path"));

        tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new ConsumeKvSnapshotForBucket(t1p0b0, 1L),
                        new ConsumeKvSnapshotForBucket(t1p0b1, 1L)));
        register("consumer2", tableIdToRegisterBucket);

        assertThat(
                        snapshotConsumerExists(
                                Arrays.asList(
                                        new ConsumeKvSnapshotForBucket(t0b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t0b1, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b0, 0),
                                        new ConsumeKvSnapshotForBucket(t1p0b1, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p1b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b0, 1L),
                                        new ConsumeKvSnapshotForBucket(t1p0b1, 1L))))
                .isTrue();
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(7);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(2);

        // update consumer register.
        tableIdToRegisterBucket = new HashMap<>();
        zookeeperClient.registerTableBucketSnapshot(t0b0, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Collections.singletonList(new ConsumeKvSnapshotForBucket(t0b0, 1L)));
        register("consumer1", tableIdToRegisterBucket);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(7);

        // new insert.
        tableIdToRegisterBucket = new HashMap<>();
        TableBucket newTableBucket = new TableBucket(DATA1_TABLE_ID_PK, 2);

        zookeeperClient.registerTableBucketSnapshot(
                newTableBucket, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Collections.singletonList(new ConsumeKvSnapshotForBucket(newTableBucket, 1L)));
        register("consumer1", tableIdToRegisterBucket);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(8);

        // unregister
        Map<Long, List<TableBucket>> tableIdToUnregisterBucket = new HashMap<>();
        tableIdToUnregisterBucket.put(DATA1_TABLE_ID_PK, Collections.singletonList(newTableBucket));
        unregister("consumer1", tableIdToUnregisterBucket);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(7);

        // unregister an non-exist bucket.
        tableIdToUnregisterBucket = new HashMap<>();
        tableIdToUnregisterBucket.put(
                DATA1_TABLE_ID_PK,
                Collections.singletonList(new TableBucket(DATA1_TABLE_ID_PK, PARTITION_ID_1, 2)));
        unregister("consumer1", tableIdToUnregisterBucket);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(7);

        // check detail content for consumer1.
        Map<Long, Long[]> tableIdToSnapshots = new HashMap<>();
        Map<Long, Set<Long>> tableIdToPartitions = new HashMap<>();
        Map<Long, Long[]> partitionIdToSnapshots = new HashMap<>();
        tableIdToSnapshots.put(DATA1_TABLE_ID_PK, new Long[] {1L, 0L, -1L});
        tableIdToPartitions.put(
                PARTITION_TABLE_ID, new HashSet<>(Arrays.asList(PARTITION_ID_1, PARTITION_ID_2)));
        partitionIdToSnapshots.put(PARTITION_ID_1, new Long[] {0L, 0L, -1L});
        partitionIdToSnapshots.put(PARTITION_ID_2, new Long[] {0L, -1L, -1L});
        KvSnapshotConsumer expectedConsumer =
                new KvSnapshotConsumer(
                        manualClock.milliseconds() + 1000L,
                        tableIdToSnapshots,
                        tableIdToPartitions,
                        partitionIdToSnapshots);
        assertThat(kvSnapshotConsumerManager.getKvSnapshotConsumer("consumer1"))
                .isEqualTo(expectedConsumer);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer1")).hasValue(expectedConsumer);

        // check detail content for consumer2.
        tableIdToSnapshots = new HashMap<>();
        tableIdToPartitions = new HashMap<>();
        partitionIdToSnapshots = new HashMap<>();
        tableIdToPartitions.put(PARTITION_TABLE_ID, Collections.singleton(PARTITION_ID_1));
        partitionIdToSnapshots.put(PARTITION_ID_1, new Long[] {1L, 1L, -1L});
        KvSnapshotConsumer expectedConsumer2 =
                new KvSnapshotConsumer(
                        manualClock.milliseconds() + 1000L,
                        tableIdToSnapshots,
                        tableIdToPartitions,
                        partitionIdToSnapshots);
        assertThat(kvSnapshotConsumerManager.getKvSnapshotConsumer("consumer2"))
                .isEqualTo(expectedConsumer2);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer2")).hasValue(expectedConsumer2);
    }

    @Test
    void testUnregisterAll() throws Exception {
        Map<Long, List<ConsumeKvSnapshotForBucket>> tableIdToRegisterBucket = initRegisterBuckets();
        register("consumer1", tableIdToRegisterBucket);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(5);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(1);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer1")).isPresent();

        Map<Long, List<TableBucket>> tableIdToUnregisterBucket = new HashMap<>();
        tableIdToUnregisterBucket.put(DATA1_TABLE_ID_PK, Arrays.asList(t0b0, t0b1));
        tableIdToUnregisterBucket.put(PARTITION_TABLE_ID, Arrays.asList(t1p0b0, t1p0b1, t1p1b0));

        // unregister all will clear this consumer.
        unregister("consumer1", tableIdToUnregisterBucket);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(0);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer1")).isNotPresent();
    }

    @Test
    void testClear() throws Exception {
        Map<Long, List<ConsumeKvSnapshotForBucket>> tableIdToRegisterBucket = initRegisterBuckets();
        register("consumer1", tableIdToRegisterBucket);

        // first register snapshot to zk.
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b0, new BucketSnapshot(1L, 10L, "test-path"));
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b1, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new ConsumeKvSnapshotForBucket(t0b0, 0L), // same ref.
                        new ConsumeKvSnapshotForBucket(t1p0b0, 1L),
                        new ConsumeKvSnapshotForBucket(t1p0b1, 1L)));
        register("consumer2", tableIdToRegisterBucket);

        assertThat(
                        snapshotConsumerExists(
                                Arrays.asList(
                                        new ConsumeKvSnapshotForBucket(t0b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t0b1, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b1, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p1b0, 0L),
                                        new ConsumeKvSnapshotForBucket(t1p0b0, 1L),
                                        new ConsumeKvSnapshotForBucket(t1p0b1, 1L))))
                .isTrue();
        assertThat(kvSnapshotConsumerManager.getRefCount(new ConsumeKvSnapshotForBucket(t0b0, 0L)))
                .isEqualTo(2);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(8);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(2);

        kvSnapshotConsumerManager.clear("consumer1");
        assertThat(kvSnapshotConsumerManager.getRefCount(new ConsumeKvSnapshotForBucket(t0b0, 0L)))
                .isEqualTo(1);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(3);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(1);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer1")).isEmpty();

        kvSnapshotConsumerManager.clear("consumer2");
        assertThat(kvSnapshotConsumerManager.getRefCount(new ConsumeKvSnapshotForBucket(t0b0, 0L)))
                .isEqualTo(0);
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(0);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer2")).isEmpty();

        assertThat(kvSnapshotConsumerManager.clear("non-exist")).isFalse();
    }

    @Test
    void testExpireConsumers() throws Exception {
        // test consumer expire by expire thread.
        Map<Long, List<ConsumeKvSnapshotForBucket>> tableIdToRegisterBucket = initRegisterBuckets();

        // expire after 1000ms.
        kvSnapshotConsumerManager.register("consumer1", 1000L, tableIdToRegisterBucket);

        tableIdToRegisterBucket = new HashMap<>();
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b0, new BucketSnapshot(1L, 10L, "test-path"));
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b1, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new ConsumeKvSnapshotForBucket(t0b0, 0L), // same ref.
                        new ConsumeKvSnapshotForBucket(t1p0b0, 1L),
                        new ConsumeKvSnapshotForBucket(t1p0b1, 1L)));
        // expire after 2000ms.
        kvSnapshotConsumerManager.register("consumer2", 2000L, tableIdToRegisterBucket);

        clearConsumerScheduler.triggerPeriodicScheduledTasks();
        // no consumer expire.
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(8);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(2);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer1")).isPresent();
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer2")).isPresent();

        manualClock.advanceTime(1005L, TimeUnit.MILLISECONDS);
        clearConsumerScheduler.triggerPeriodicScheduledTasks();
        // consumer1 expire.
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(3);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(1);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer1")).isNotPresent();
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer2")).isPresent();

        manualClock.advanceTime(1005L, TimeUnit.MILLISECONDS);
        clearConsumerScheduler.triggerPeriodicScheduledTasks();
        // consumer2 expire.
        assertThat(kvSnapshotConsumerManager.getConsumedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotConsumerManager.getConsumerCount()).isEqualTo(0);
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer1")).isNotPresent();
        assertThat(zookeeperClient.getKvSnapshotConsumer("consumer2")).isNotPresent();
    }

    @Test
    void registerWithNotExistSnapshotId() throws Exception {
        Map<Long, List<ConsumeKvSnapshotForBucket>> tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Arrays.asList(
                        new ConsumeKvSnapshotForBucket(t0b0, 1000L),
                        new ConsumeKvSnapshotForBucket(t0b1, 1000L)));
        assertThat(kvSnapshotConsumerManager.register("consumer1", 1000L, tableIdToRegisterBucket))
                .contains(t0b0);
    }

    private void initCoordinatorContext() {
        coordinatorContext = new CoordinatorContext();
        coordinatorContext.setLiveTabletServers(createServers(Arrays.asList(0, 1, 2)));

        // register an non-partitioned table.
        coordinatorContext.putTableInfo(DATA1_TABLE_INFO_PK);
        coordinatorContext.putTablePath(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK);

        // register a partitioned table.
        coordinatorContext.putTableInfo(PARTITION_TABLE_INFO);
        coordinatorContext.putTablePath(
                PARTITION_TABLE_INFO.getTableId(), PARTITION_TABLE_INFO.getTablePath());
        coordinatorContext.putPartition(PARTITION_ID_1, PARTITION_TABLE_PATH_1);
        coordinatorContext.putPartition(PARTITION_ID_2, PARTITION_TABLE_PATH_2);
    }

    private Map<Long, List<ConsumeKvSnapshotForBucket>> initRegisterBuckets() {
        Map<Long, List<ConsumeKvSnapshotForBucket>> tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Arrays.asList(
                        new ConsumeKvSnapshotForBucket(t0b0, 0L),
                        new ConsumeKvSnapshotForBucket(t0b1, 0L)));
        tableIdToRegisterBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new ConsumeKvSnapshotForBucket(t1p0b0, 0L),
                        new ConsumeKvSnapshotForBucket(t1p0b1, 0L),
                        new ConsumeKvSnapshotForBucket(t1p1b0, 0L)));
        return tableIdToRegisterBucket;
    }

    private boolean snapshotConsumerNotExists(List<ConsumeKvSnapshotForBucket> bucketList) {
        return bucketList.stream()
                .allMatch(bucket -> kvSnapshotConsumerManager.snapshotConsumerNotExist(bucket));
    }

    private boolean snapshotConsumerExists(List<ConsumeKvSnapshotForBucket> bucketList) {
        return bucketList.stream()
                .noneMatch(bucket -> kvSnapshotConsumerManager.snapshotConsumerNotExist(bucket));
    }

    private void register(
            String consumerId, Map<Long, List<ConsumeKvSnapshotForBucket>> tableIdToRegisterBucket)
            throws Exception {
        kvSnapshotConsumerManager.register(consumerId, 1000L, tableIdToRegisterBucket);
    }

    private void unregister(
            String consumerId, Map<Long, List<TableBucket>> tableIdToUnregisterBucket)
            throws Exception {
        kvSnapshotConsumerManager.unregister(consumerId, tableIdToUnregisterBucket);
    }

    private long register(
            KvSnapshotConsumer consumer, ConsumeKvSnapshotForBucket consumeKvSnapshotForBucket) {
        return consumer.registerBucket(
                consumeKvSnapshotForBucket.getTableBucket(),
                consumeKvSnapshotForBucket.getKvSnapshotId(),
                NUM_BUCKETS);
    }
}
