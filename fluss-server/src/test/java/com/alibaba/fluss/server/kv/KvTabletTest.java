/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.InvalidTargetColumnException;
import com.alibaba.fluss.exception.OutOfOrderSequenceException;
import com.alibaba.fluss.memory.TestingMemorySegmentPool;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.KvRecord;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.KvRecordTestUtils;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.LogTestBase;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.record.TestData;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer.KvEntry;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer.Value;
import com.alibaba.fluss.server.log.FetchIsolation;
import com.alibaba.fluss.server.log.LogAppendInfo;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.log.LogTestUtils;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.alibaba.fluss.record.LogRecordBatch.NO_BATCH_SEQUENCE;
import static com.alibaba.fluss.record.LogRecordBatch.NO_WRITER_ID;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA_PK;
import static com.alibaba.fluss.record.TestData.DATA2_SCHEMA;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

/** Test for {@link KvTablet}. */
class KvTabletTest {

    private static final short schemaId = 1;
    private final Configuration conf = new Configuration();
    private final RowType baseRowType = TestData.DATA1_ROW_TYPE;
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(schemaId);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(baseRowType);

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private LogTablet logTablet;
    private KvTablet kvTablet;
    private ExecutorService executor;

    @BeforeEach
    void beforeEach() throws Exception {
        PhysicalTablePath tablePath = PhysicalTablePath.of(TablePath.of("testDb", "t1"));
        long tableId = 0L;
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir, tablePath.getDatabaseName(), tableId, tablePath.getTableName());
        logTablet =
                LogTablet.create(
                        tablePath,
                        logTabletDir,
                        conf,
                        0,
                        new FlussScheduler(1),
                        LogFormat.ARROW,
                        1,
                        true,
                        SystemClock.getInstance());
        TableBucket tableBucket = logTablet.getTableBucket();
        kvTablet =
                KvTablet.create(
                        tablePath,
                        tableBucket,
                        logTablet,
                        tmpKvDir,
                        conf,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED);
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void afterEach() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    void testInvalidPartialUpdate() throws Exception {
        final Schema schema1 = DATA2_SCHEMA;
        KvRecordTestUtils.KvRecordFactory data2kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(schema1.toRowType());
        KvRecordBatch kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Collections.singletonList(
                                data2kvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, null, null})));
        // target columns don't contains primary key columns
        assertThatThrownBy(() -> kvTablet.putAsLeader(kvRecordBatch, new int[] {1, 2}, schema1))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "The target write columns [b, c] must contain the primary key columns [a].");

        // the column not in target columns is not null
        final Schema schema2 =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", new StringType(false))
                        .primaryKey("a")
                        .build();
        assertThatThrownBy(() -> kvTablet.putAsLeader(kvRecordBatch, new int[] {0, 1}, schema2))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "Partial Update requires all columns except primary key to be nullable, but column c is NOT NULL.");
        assertThatThrownBy(() -> kvTablet.putAsLeader(kvRecordBatch, new int[] {0, 2}, schema2))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "Partial Update requires all columns except primary key to be nullable, but column c is NOT NULL.");
    }

    @Test
    void testPartialUpdateAndDelete() throws Exception {
        Schema dataSchema = DATA2_SCHEMA;
        RowType rowType = DATA2_SCHEMA.toRowType();
        KvRecordTestUtils.KvRecordFactory data2kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(rowType);

        // create one kv batch with only writing first one column
        KvRecordBatch kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Arrays.asList(
                                data2kvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, null, null}),
                                data2kvRecordFactory.ofRecord(
                                        "k2".getBytes(), new Object[] {2, null, null}),
                                data2kvRecordFactory.ofRecord(
                                        "k2".getBytes(), new Object[] {2, null, null})));

        int[] targetColumns = new int[] {0};

        kvTablet.putAsLeader(kvRecordBatch, targetColumns, dataSchema);
        long endOffset = logTablet.localLogEndOffset();

        // expected cdc log records for putting order: batch1, batch2;
        List<MemoryLogRecords> expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                0,
                                Arrays.asList(
                                        // -- for batch 1
                                        RowKind.INSERT,
                                        RowKind.INSERT,
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER),
                                Arrays.asList(
                                        // for k1
                                        new Object[] {1, null, null},
                                        // for k2: +I
                                        new Object[] {2, null, null},
                                        // for k2: -U, +U
                                        new Object[] {2, null, null},
                                        new Object[] {2, null, null})));

        LogRecords actualLogRecords = readLogRecords();

        checkEqual(actualLogRecords, expectedLogs, rowType);

        // create one kv batch with only writing second one column
        KvRecordBatch kvRecordBatch2 =
                kvRecordBatchFactory.ofRecords(
                        Arrays.asList(
                                data2kvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, "v11", null}),
                                data2kvRecordFactory.ofRecord(
                                        "k2".getBytes(), new Object[] {2, "v21", null}),
                                data2kvRecordFactory.ofRecord(
                                        "k2".getBytes(), new Object[] {2, "v23", null})));

        targetColumns = new int[] {0, 1};
        kvTablet.putAsLeader(kvRecordBatch2, targetColumns, dataSchema);

        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Arrays.asList(
                                        // -- for k1
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER,
                                        // -- for k2
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER,
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER),
                                Arrays.asList(
                                        // for k1
                                        new Object[] {1, null, null},
                                        new Object[] {1, "v11", null},
                                        // for k2: -U, +U
                                        new Object[] {2, null, null},
                                        new Object[] {2, "v21", null},
                                        // for k2: -U, +U
                                        new Object[] {2, "v21", null},
                                        new Object[] {2, "v23", null})));
        actualLogRecords = readLogRecords(endOffset);
        checkEqual(actualLogRecords, expectedLogs, rowType);
        endOffset = logTablet.localLogEndOffset();

        // now, not use partial update, update all columns, it should still work
        KvRecordBatch kvRecordBatch3 =
                kvRecordBatchFactory.ofRecords(
                        Arrays.asList(
                                data2kvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, null, "c3"}),
                                data2kvRecordFactory.ofRecord(
                                        "k3".getBytes(), new Object[] {3, null, "v211"})));

        targetColumns = new int[] {0, 2};
        kvTablet.putAsLeader(kvRecordBatch3, targetColumns, dataSchema);

        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Arrays.asList(
                                        // -- for k1
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER,
                                        // -- for k3
                                        RowKind.INSERT),
                                Arrays.asList(
                                        // for k1
                                        new Object[] {1, "v11", null},
                                        new Object[] {1, "v11", "c3"},
                                        // for k3: +I
                                        new Object[] {3, null, "v211"})));
        actualLogRecords = readLogRecords(endOffset);

        checkEqual(actualLogRecords, expectedLogs, rowType);

        endOffset = logTablet.localLogEndOffset();

        // now, partial delete "k1", delete column 1
        targetColumns = new int[] {0, 1};
        kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Collections.singletonList(
                                data2kvRecordFactory.ofRecord("k1".getBytes(), null)));
        kvTablet.putAsLeader(kvRecordBatch, targetColumns, dataSchema);

        // check cdc log, should produce -U, +U
        actualLogRecords = readLogRecords(endOffset);
        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Arrays.asList(
                                        // -- for k1
                                        RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER),
                                Arrays.asList(
                                        new Object[] {1, "v11", "c3"},
                                        new Object[] {1, null, "c3"})));
        checkEqual(actualLogRecords, expectedLogs, rowType);
        // we can still get "k1" from pre-write buffer
        assertThat(kvTablet.getKvPreWriteBuffer().get(Key.of("k1".getBytes())))
                .isEqualTo(valueOf(compactedRow(rowType, new Object[] {1, null, "c3"})));

        endOffset = logTablet.localLogEndOffset();
        // now, partial delete "k1", delete column 2, should produce -D
        targetColumns = new int[] {0, 2};
        kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Collections.singletonList(
                                data2kvRecordFactory.ofRecord("k1".getBytes(), null)));
        kvTablet.putAsLeader(kvRecordBatch, targetColumns, dataSchema);
        // check cdc log, should produce -U, +U
        actualLogRecords = readLogRecords(endOffset);
        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Collections.singletonList(
                                        // -- for k1
                                        RowKind.DELETE),
                                Collections.singletonList(new Object[] {1, null, "c3"})));
        checkEqual(actualLogRecords, expectedLogs, rowType);
        // now we can not get "k1" from pre-write buffer
        assertThat(kvTablet.getKvPreWriteBuffer().get(Key.of("k1".getBytes()))).isNotNull();
    }

    @Test
    void testPutWithMultiThread() throws Exception {
        // create two kv batches
        KvRecordBatch kvRecordBatch1 =
                kvRecordBatchFactory.ofRecords(
                        Arrays.asList(
                                kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v11"}),
                                kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v21"}),
                                kvRecordFactory.ofRecord(
                                        "k2".getBytes(), new Object[] {2, "v23"})));

        KvRecordBatch kvRecordBatch2 =
                kvRecordBatchFactory.ofRecords(
                        Arrays.asList(
                                kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v22"}),
                                kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v21"}),
                                kvRecordFactory.ofRecord("k1".getBytes(), null)));

        // expected cdc log records for putting order: batch1, batch2;
        List<MemoryLogRecords> expectedLogs1 =
                Arrays.asList(
                        logRecords(
                                0L,
                                Arrays.asList(
                                        // -- for batch 1
                                        RowKind.INSERT,
                                        RowKind.INSERT,
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER),
                                Arrays.asList(
                                        // --- cdc log for batch 1
                                        // for k1
                                        new Object[] {1, "v11"},
                                        // for k2: +I
                                        new Object[] {2, "v21"},
                                        // for k2: -U, +U
                                        new Object[] {2, "v21"},
                                        new Object[] {2, "v23"})),
                        logRecords(
                                4L,
                                Arrays.asList(
                                        // -- for batch2
                                        // for k2
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER,
                                        // for k1
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER,
                                        RowKind.DELETE),
                                Arrays.asList(
                                        // -- cdc log for batch 2
                                        // for k2: -U, +U
                                        new Object[] {2, "v23"},
                                        new Object[] {2, "v22"},
                                        // for k1:  -U, +U
                                        new Object[] {1, "v11"},
                                        new Object[] {1, "v21"},
                                        // for k1: -D
                                        new Object[] {1, "v21"})));
        // expected log last offset for each putting with putting order: batch1, batch2
        List<Long> expectLogLastOffset1 = Arrays.asList(3L, 8L);

        // expected cdc log records for putting order: batch2, batch1;
        List<MemoryLogRecords> expectedLogs2 =
                Arrays.asList(
                        logRecords(
                                0L,
                                Arrays.asList(RowKind.INSERT, RowKind.INSERT, RowKind.DELETE),
                                Arrays.asList(
                                        // --- cdc log for batch 2
                                        // for k2
                                        new Object[] {2, "v22"},
                                        // for k1
                                        new Object[] {1, "v21"},
                                        new Object[] {1, "v21"})),
                        logRecords(
                                3L,
                                Arrays.asList(
                                        RowKind.INSERT,
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER,
                                        RowKind.UPDATE_BEFORE,
                                        RowKind.UPDATE_AFTER),
                                Arrays.asList(
                                        // -- cdc log for batch 1
                                        // for k1
                                        new Object[] {1, "v11"},
                                        // for k2, -U, +U
                                        new Object[] {2, "v22"},
                                        new Object[] {2, "v21"},
                                        // for k2, -U, +U
                                        new Object[] {2, "v21"},
                                        new Object[] {2, "v23"})));
        // expected log last offset for each putting with putting order: batch2, batch1
        List<Long> expectLogLastOffset2 = Arrays.asList(2L, 7L);

        // start two thread to put records, one to put batch1, one to put batch2;
        // each thread will sleep for a random time;
        // so, the putting order can be batch1, batch2; or batch2, batch1
        Random random = new Random();
        List<Future<LogAppendInfo>> putFutures = new ArrayList<>();
        putFutures.add(
                executor.submit(
                        () -> {
                            Thread.sleep(random.nextInt(100));
                            return kvTablet.putAsLeader(kvRecordBatch1, null, DATA1_SCHEMA_PK);
                        }));
        putFutures.add(
                executor.submit(
                        () -> {
                            Thread.sleep(random.nextInt(100));
                            return kvTablet.putAsLeader(kvRecordBatch2, null, DATA1_SCHEMA_PK);
                        }));

        List<Long> actualLogEndOffset = new ArrayList<>();
        for (Future<LogAppendInfo> future : putFutures) {
            actualLogEndOffset.add(future.get().lastOffset());
        }
        Collections.sort(actualLogEndOffset);

        LogRecords actualLogRecords = readLogRecords();
        // with putting order: batch1, batch2
        if (actualLogEndOffset.equals(expectLogLastOffset1)) {
            checkEqual(actualLogRecords, expectedLogs1);
        } else if (actualLogEndOffset.equals(expectLogLastOffset2)) {
            // with putting order: batch2, batch1
            checkEqual(actualLogRecords, expectedLogs2);
        } else {
            fail(
                    "The putting order is not batch1, batch2; or batch2, batch1. The actual log end offset is: "
                            + actualLogEndOffset
                            + ". The expected log end offset is: "
                            + expectLogLastOffset1
                            + " or "
                            + expectLogLastOffset2);
        }
    }

    @Test
    void testPutAsLeaderWithOutOfOrderSequenceException() throws Exception {
        long writeId = 100L;
        List<KvRecord> kvData1 =
                Arrays.asList(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v11"}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v21"}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v23"}));
        KvRecordBatch kvRecordBatch1 = kvRecordBatchFactory.ofRecords(kvData1, writeId, 0);

        List<KvRecord> kvData2 =
                Arrays.asList(
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v22"}),
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v21"}),
                        kvRecordFactory.ofRecord("k1".getBytes(), null));
        KvRecordBatch kvRecordBatch2 = kvRecordBatchFactory.ofRecords(kvData2, writeId, 3);
        kvTablet.putAsLeader(kvRecordBatch1, null, DATA1_SCHEMA_PK);

        KvEntry kv1 =
                KvEntry.of(
                        Key.of("k1".getBytes()),
                        valueOf(compactedRow(baseRowType, new Object[] {1, "v11"})),
                        0);
        KvEntry kv2 =
                KvEntry.of(
                        Key.of("k2".getBytes()),
                        valueOf(compactedRow(baseRowType, new Object[] {2, "v21"})),
                        1);
        KvEntry kv3 =
                KvEntry.of(
                        Key.of("k2".getBytes()),
                        valueOf(compactedRow(baseRowType, new Object[] {2, "v23"})),
                        3,
                        kv2);
        List<KvEntry> expectedEntries = Arrays.asList(kv1, kv2, kv3);
        Map<Key, KvEntry> expectedMap = new HashMap<>();
        for (KvEntry kvEntry : expectedEntries) {
            expectedMap.put(kvEntry.getKey(), kvEntry);
        }
        assertThat(kvTablet.getKvPreWriteBuffer().getAllKvEntries()).isEqualTo(expectedEntries);
        assertThat(kvTablet.getKvPreWriteBuffer().getKvEntryMap()).isEqualTo(expectedMap);
        assertThat(kvTablet.getKvPreWriteBuffer().getMaxLSN()).isEqualTo(3);

        // the second batch will be ignored.
        assertThatThrownBy(() -> kvTablet.putAsLeader(kvRecordBatch2, null, DATA1_SCHEMA_PK))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 100 at offset 8 in table-bucket "
                                + "TableBucket{tableId=0, bucket=587113} : 3 (incoming batch seq.), 0 (current batch seq.)");
        assertThat(kvTablet.getKvPreWriteBuffer().getAllKvEntries()).isEqualTo(expectedEntries);
        assertThat(kvTablet.getKvPreWriteBuffer().getKvEntryMap()).isEqualTo(expectedMap);
        assertThat(kvTablet.getKvPreWriteBuffer().getMaxLSN()).isEqualTo(3);
    }

    private LogRecords readLogRecords() throws Exception {
        return readLogRecords(0L);
    }

    private LogRecords readLogRecords(long startOffset) throws Exception {
        return logTablet
                .read(startOffset, Integer.MAX_VALUE, FetchIsolation.LOG_END, false, null)
                .getRecords();
    }

    private MemoryLogRecords logRecords(
            long baseOffset, List<RowKind> rowKinds, List<Object[]> values) throws Exception {
        return createBasicMemoryLogRecords(
                baseRowType,
                DEFAULT_SCHEMA_ID,
                baseOffset,
                -1L,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                rowKinds,
                values,
                LogFormat.ARROW);
    }

    private MemoryLogRecords logRecords(
            RowType rowType, long baseOffset, List<RowKind> rowKinds, List<Object[]> values)
            throws Exception {
        return createBasicMemoryLogRecords(
                rowType,
                DEFAULT_SCHEMA_ID,
                baseOffset,
                -1L,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                rowKinds,
                values,
                LogFormat.ARROW);
    }

    private void checkEqual(
            LogRecords actaulLogRecords, List<MemoryLogRecords> expectedLogs, RowType rowType) {
        LogTestBase.assertLogRecordsListEquals(expectedLogs, actaulLogRecords, rowType);
    }

    private void checkEqual(LogRecords actaulLogRecords, List<MemoryLogRecords> expectedLogs) {
        LogTestBase.assertLogRecordsListEquals(expectedLogs, actaulLogRecords, baseRowType);
    }

    private Value valueOf(BinaryRow row) {
        return Value.of(ValueEncoder.encodeValue(schemaId, row));
    }
}
