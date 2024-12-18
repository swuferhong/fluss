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

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A queue that buffers the pending lookup operations and provides a list of {@link Lookup} when
 * call method {@link #drain(LookupType)}.
 */
@ThreadSafe
@Internal
class LookupQueue {

    private volatile boolean closed;
    private final ArrayBlockingQueue<AbstractLookup> lookupQueue;
    private final ArrayBlockingQueue<AbstractLookup> indexLookupQueue;
    // next drain queue, 0 or 1, 0 means lookupQueue, 1 means indexLookupQueue.
    private final AtomicInteger nextDrainQueue;
    private final int maxBatchSize;
    private final long batchTimeoutMs;

    LookupQueue(Configuration conf) {
        this.lookupQueue =
                new ArrayBlockingQueue<>(conf.get(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE));
        this.indexLookupQueue =
                new ArrayBlockingQueue<>(conf.get(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE));
        this.nextDrainQueue = new AtomicInteger(0);
        this.maxBatchSize = conf.get(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE);
        this.batchTimeoutMs = conf.get(ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT).toMillis();
        this.closed = false;
    }

    LookupType nexDrainLookupType() {
        int drainQueueId = nextDrainQueue.updateAndGet(i -> i == 0 ? 1 : 0);
        if (drainQueueId == 0) {
            return LookupType.LOOKUP;
        } else {
            return LookupType.INDEX_LOOKUP;
        }
    }

    void appendLookup(AbstractLookup lookup) {
        if (closed) {
            throw new IllegalStateException(
                    "Can not append lookup operation since the LookupQueue is closed.");
        }
        try {
            if (lookup.lookupType() == LookupType.LOOKUP) {
                lookupQueue.put(lookup);
            } else {
                indexLookupQueue.put(lookup);
            }
        } catch (InterruptedException e) {
            lookup.future().completeExceptionally(e);
        }
    }

    boolean hasUnDrained() {
        return !lookupQueue.isEmpty();
    }

    /** Drain a batch of {@link Lookup}s from the lookup queue. */
    List<AbstractLookup> drain(LookupType lookupType) throws Exception {
        long start = System.currentTimeMillis();
        List<AbstractLookup> lookupOperations = new ArrayList<>(maxBatchSize);
        ArrayBlockingQueue<AbstractLookup> queue = getQueue(lookupType);
        int count = 0;
        while (true) {
            if (System.currentTimeMillis() - start > batchTimeoutMs) {
                break;
            }

            AbstractLookup lookup = queue.poll(300, TimeUnit.MILLISECONDS);
            if (lookup == null) {
                break;
            }
            count++;
            lookupOperations.add(lookup);
            if (count >= maxBatchSize) {
                break;
            }
        }
        return lookupOperations;
    }

    /** Drain all the {@link Lookup}s from the lookup queue. */
    List<AbstractLookup> drainAll(LookupType lookupType) {
        ArrayBlockingQueue<AbstractLookup> queue = getQueue(lookupType);
        List<AbstractLookup> lookupOperations = new ArrayList<>(queue.size());
        queue.drainTo(lookupOperations);
        return lookupOperations;
    }

    private ArrayBlockingQueue<AbstractLookup> getQueue(LookupType lookupType) {
        if (lookupType == LookupType.LOOKUP) {
            return this.lookupQueue;
        } else {
            return this.indexLookupQueue;
        }
    }

    public void close() {
        closed = true;
    }
}
