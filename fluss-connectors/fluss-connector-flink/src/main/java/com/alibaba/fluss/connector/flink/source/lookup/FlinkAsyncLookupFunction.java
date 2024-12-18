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

package com.alibaba.fluss.connector.flink.source.lookup;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.source.lookup.LookupNormalizer.RemainingFilter;
import com.alibaba.fluss.connector.flink.utils.FlinkConversions;
import com.alibaba.fluss.connector.flink.utils.FlinkRowToFlussRowConverter;
import com.alibaba.fluss.connector.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A flink async lookup function for fluss. */
public class FlinkAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkAsyncLookupFunction.class);

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final int maxRetryTimes;
    private final RowType flinkRowType;
    private final int[] lookupKeyIndexes;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private final int[] projection;

    private transient FlinkRowToFlussRowConverter flinkRowToFlussRowConverter;
    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient Connection connection;
    private transient Table table;

    public FlinkAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            int[] lookupKeyIndexes,
            int maxRetryTimes,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.maxRetryTimes = maxRetryTimes;
        this.flinkRowType = flinkRowType;
        this.lookupKeyIndexes = lookupKeyIndexes;
        this.lookupNormalizer = lookupNormalizer;
        this.projection = projection;
    }

    private RowType toLookupKeyRowType(RowType rowType, int[] lookupKeyIndex) {
        LogicalType[] types = new LogicalType[lookupKeyIndex.length];
        String[] names = new String[lookupKeyIndex.length];
        for (int i = 0; i < lookupKeyIndex.length; i++) {
            types[i] = rowType.getTypeAt(lookupKeyIndex[i]);
            names[i] = rowType.getFieldNames().get(lookupKeyIndex[i]);
        }
        return RowType.of(rowType.isNullable(), types, names);
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        connection = ConnectionFactory.createConnection(flussConfig);
        table = connection.getTable(tablePath);
        // TODO: convert to Fluss GenericRow to avoid unnecessary deserialization
        flinkRowToFlussRowConverter =
                FlinkRowToFlussRowConverter.create(
                        toLookupKeyRowType(flinkRowType, lookupKeyIndexes),
                        table.getDescriptor().getKvFormat());
        flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(FlinkConversions.toFlussRowType(flinkRowType));
        LOG.info("end open.");
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param keyRow A {@link RowData} that wraps lookup keys.
     */
    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        RemainingFilter remainingFilter = lookupNormalizer.createRemainingFilter(keyRow);
        InternalRow flussKeyRow = flinkRowToFlussRowConverter.toInternalRow(normalizedKeyRow);
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        // fetch result
        fetchResult(future, 0, flussKeyRow, remainingFilter, lookupNormalizer.getIndexName());
        return future;
    }

    /**
     * Execute async fetch result .
     *
     * @param resultFuture The result or exception is returned.
     * @param currentRetry Current number of retries.
     * @param keyRow the key row to get.
     * @param remainingFilter the nullable remaining filter to filter the result.
     * @param indexName the index name to get.
     */
    private void fetchResult(
            CompletableFuture<Collection<RowData>> resultFuture,
            int currentRetry,
            InternalRow keyRow,
            @Nullable RemainingFilter remainingFilter,
            @Nullable String indexName) {
        CompletableFuture<LookupResult> responseFuture;
        if (indexName == null) {
            responseFuture = table.lookup(keyRow);
        } else {
            responseFuture = table.indexLookup(indexName, keyRow);
        }

        responseFuture.whenComplete(
                (result, throwable) -> {
                    if (throwable != null) {
                        if (throwable instanceof TableNotExistException) {
                            LOG.error("Table '{}' not found ", tablePath, throwable);
                            resultFuture.completeExceptionally(
                                    new RuntimeException(
                                            "Fluss table '" + tablePath + "' not found.",
                                            throwable));
                        } else {
                            LOG.error(
                                    "Fluss asyncLookup error, retry times = {}",
                                    currentRetry,
                                    throwable);
                            if (currentRetry >= maxRetryTimes) {
                                String exceptionMsg =
                                        String.format(
                                                "Execution of Fluss asyncLookup failed: %s, retry times = %d.",
                                                throwable.getMessage(), currentRetry);
                                resultFuture.completeExceptionally(
                                        new RuntimeException(exceptionMsg, throwable));
                            } else {
                                try {
                                    Thread.sleep(1000L * currentRetry);
                                } catch (InterruptedException e1) {
                                    resultFuture.completeExceptionally(e1);
                                }
                                fetchResult(
                                        resultFuture,
                                        currentRetry + 1,
                                        keyRow,
                                        remainingFilter,
                                        indexName);
                            }
                        }
                    } else {
                        List<RowData> projectedRow = new ArrayList<>();
                        for (InternalRow row : result.getRowList()) {
                            if (row != null) {
                                RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(row);
                                if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                                    projectedRow.add(maybeProject(flinkRow));
                                }
                            }
                        }
                        resultFuture.complete(projectedRow);
                    }
                });
    }

    private RowData maybeProject(RowData row) {
        if (projection == null) {
            return row;
        }
        // should not reuse objects for async operations
        return ProjectedRowData.from(projection).replaceRow(row);
    }

    @Override
    public void close() throws Exception {
        LOG.info("start close ...");
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
        LOG.info("end close.");
    }
}
