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

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.ArrowUtils;
import com.alibaba.fluss.utils.Projection;

import javax.annotation.Nullable;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** A simple implementation for {@link LogRecordBatch.ReadContext}. */
public class LogRecordReadContext implements LogRecordBatch.ReadContext, AutoCloseable {

    // the log format of the table
    private final LogFormat logFormat;
    // the static schema of the table
    private final RowType rowType;
    // the static schemaId of the table, should support dynamic schema evolution in the future
    private final int schemaId;
    // the Arrow vector schema root of the table, should be null if not ARROW log format
    @Nullable private final VectorSchemaRoot vectorSchemaRoot;
    // the Arrow memory buffer allocator for the table, should be null if not ARROW log format
    @Nullable private final BufferAllocator bufferAllocator;

    /**
     * Creates a LogRecordReadContext for the given table information and projection information.
     */
    public static LogRecordReadContext createReadContext(
            TableInfo tableInfo, @Nullable Projection projection) {
        TableDescriptor desc = tableInfo.getTableDescriptor();
        RowType rowType = desc.getSchema().toRowType();
        LogFormat logFormat = desc.getLogFormat();
        int schemaId = tableInfo.getSchemaId();

        if (logFormat == LogFormat.ARROW) {
            // TODO: use a more reasonable memory limit
            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            if (projection == null) {
                VectorSchemaRoot vectorRoot =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
                return createArrowReadContext(rowType, schemaId, vectorRoot, allocator);
            } else {
                RowType projectedRowType = projection.projectInOrder(rowType);
                VectorSchemaRoot vectorRoot =
                        VectorSchemaRoot.create(
                                ArrowUtils.toArrowSchema(projectedRowType), allocator);
                return createArrowReadContext(projectedRowType, schemaId, vectorRoot, allocator);
            }
        } else if (logFormat == LogFormat.INDEXED) {
            return createIndexedReadContext(rowType, schemaId);
        } else {
            throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    /**
     * Creates a LogRecordReadContext for ARROW log format.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     * @param vectorSchemaRoot the shared vector schema root for the table
     * @param bufferAllocator the shared buffer allocator for the table
     */
    public static LogRecordReadContext createArrowReadContext(
            RowType rowType,
            int schemaId,
            VectorSchemaRoot vectorSchemaRoot,
            BufferAllocator bufferAllocator) {
        checkNotNull(vectorSchemaRoot);
        checkNotNull(bufferAllocator);
        return new LogRecordReadContext(
                LogFormat.ARROW, rowType, schemaId, vectorSchemaRoot, bufferAllocator);
    }

    /**
     * Creates a testing purpose LogRecordReadContext for ARROW log format, that underlying Arrow
     * resources are not reused.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     */
    @VisibleForTesting
    public static LogRecordReadContext createArrowReadContext(RowType rowType, int schemaId) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot vectorRoot =
                VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
        return createArrowReadContext(rowType, schemaId, vectorRoot, allocator);
    }

    /**
     * Creates a LogRecordReadContext for INDEXED log format.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     */
    public static LogRecordReadContext createIndexedReadContext(RowType rowType, int schemaId) {
        return new LogRecordReadContext(LogFormat.INDEXED, rowType, schemaId, null, null);
    }

    private LogRecordReadContext(
            LogFormat logFormat,
            RowType rowType,
            int schemaId,
            VectorSchemaRoot vectorSchemaRoot,
            BufferAllocator bufferAllocator) {
        this.logFormat = logFormat;
        this.rowType = rowType;
        this.schemaId = schemaId;
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.bufferAllocator = bufferAllocator;
    }

    @Override
    public LogFormat getLogFormat() {
        return logFormat;
    }

    @Override
    public RowType getRowType(int schemaId) {
        checkArgument(
                schemaId == this.schemaId,
                "The schemaId (%s) in the record batch is not the same as the context (%s).",
                schemaId,
                this.schemaId);
        return rowType;
    }

    public RowType getRowType() {
        return rowType;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot(int schemaId) {
        checkArgument(
                schemaId == this.schemaId,
                "The schemaId (%s) in the record batch is not the same as the context (%s).",
                schemaId,
                this.schemaId);
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException(
                    "Only Arrow log format provides vector schema root.");
        }
        checkNotNull(vectorSchemaRoot, "The vector schema root is not available.");
        return vectorSchemaRoot;
    }

    @Override
    public BufferAllocator getBufferAllocator() {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException("Only Arrow log format provides buffer allocator.");
        }
        checkNotNull(bufferAllocator, "The buffer allocator is not available.");
        return bufferAllocator;
    }

    public void close() {
        if (vectorSchemaRoot != null) {
            vectorSchemaRoot.close();
        }
        if (bufferAllocator != null) {
            bufferAllocator.close();
        }
    }
}
