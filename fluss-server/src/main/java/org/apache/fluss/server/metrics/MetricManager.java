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

package org.apache.fluss.server.metrics;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Designed to manage and clean up metrics which will be expired. To be more specific, we may have
 * three kind of metrics: 1. Never expired metrics, such as "tableCount". It's value will change but
 * the metric itself is persistent. 2. Expired but autonomous metrics, such as "bytesInPerSecond"
 * for a table. Once the table was dropped, the metric will be deleted. So we don't need to worry
 * about memory leak. 3. Expired and free metrics, mostly as a user level metric, such as
 * "bytesInCount" or "bytesOutCount" for a user. Users may start and stop to write/read a table
 * anytime so that we can't predict when to free the metric. In such scene, to avoid memory leak, we
 * design MetricManager to clean up them periodically if a metric is inactive and reach an
 * expiration time.
 */
public class MetricManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MetricManager.class);

    private final long inactiveMetricExpirationTimeMs;
    private final ScheduledExecutorService metricsScheduler;
    private final ConcurrentMap<String, AbstractMetricGroup> metrics =
            MapUtils.newConcurrentHashMap();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public MetricManager(Configuration configuration) {
        this.inactiveMetricExpirationTimeMs =
                configuration
                        .get(ConfigOptions.METRICS_MANAGER_INACTIVE_EXPIRATION_TIME)
                        .toMillis();

        this.metricsScheduler =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("periodic-metric-cleanup-manager"));
        this.metricsScheduler.scheduleAtFixedRate(
                new ExpiredMetricCleanupTask(), 30, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractMetricGroup> T getOrCreateMetric(
            String key, Function<String, T> mappingFunction) {
        checkNotClosed();

        return (T) metrics.computeIfAbsent(key, mappingFunction);
    }

    @VisibleForTesting
    public AbstractMetricGroup getMetric(String key) {
        return metrics.get(key);
    }

    public void removeMetric(String name) {
        checkNotClosed();

        AbstractMetricGroup metricGroup = metrics.remove(name);
        if (metricGroup != null) {
            metricGroup.close();
        }
    }

    public boolean hasExpired(String metricName) {
        return (System.currentTimeMillis() - metrics.get(metricName).getLastRecordTime())
                > this.inactiveMetricExpirationTimeMs;
    }

    private void checkNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("MetricManager is already closed.");
        }
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            metricsScheduler.shutdownNow();
        }
    }

    class ExpiredMetricCleanupTask implements Runnable {
        @Override
        public void run() {
            for (Map.Entry<String, AbstractMetricGroup> metricEntry : metrics.entrySet()) {
                String metricName = metricEntry.getKey();
                AbstractMetricGroup metricGroup = metricEntry.getValue();
                synchronized (metricGroup) {
                    if (hasExpired(metricName)) {
                        LOG.info("Removing expired metric {}", metricName);
                        removeMetric(metricName);
                        LOG.info("Remove expired metric {} success", metricName);
                    }
                }
            }
        }
    }
}
