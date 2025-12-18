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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.metrics.group.UserMetricGroup;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.getScopeName;
import static org.apache.fluss.server.metrics.group.TestingMetricGroups.TABLET_SERVER_METRICS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MetricManager}. */
public class MetricManagerTest {
    @Test
    void testMetricManager() throws Exception {
        Configuration conf = new Configuration();
        // set expiration time to 2s in test
        conf.set(ConfigOptions.METRICS_MANAGER_INACTIVE_EXPIRATION_TIME, Duration.ofSeconds(2));
        MetricManager metricManager = new MetricManager(conf);

        UserMetricGroup userMetricGroup = TestingMetricGroups.USER_METRICS;

        MetricManager.ExpiredMetricCleanupTask metricCleanupTask =
                metricManager.new ExpiredMetricCleanupTask();

        // fresh lastRecordTime
        userMetricGroup.incBytesIn(100);
        String metricName =
                getScopeName(userMetricGroup.getParent(), userMetricGroup.getPrincipalName());
        metricManager.getOrCreateMetric(metricName, name -> TestingMetricGroups.USER_METRICS);

        // metric successful created
        assertThat(metricManager.getMetric(metricName)).isEqualTo(TestingMetricGroups.USER_METRICS);

        // sleep 1s and fresh it, metric should not be removed
        Thread.sleep(1000);
        userMetricGroup.incBytesIn(100);
        metricCleanupTask.run();
        assertThat(metricManager.getMetric(metricName)).isNotNull();
        assertThat(userMetricGroup.isClosed()).isFalse();

        // sleep 3s and metric should already be removed
        Thread.sleep(3000);
        metricCleanupTask.run();

        // metric successful removed
        assertThat(metricManager.getMetric(metricName)).isNull();
        assertThat(userMetricGroup.isClosed()).isTrue();

        // the metric is created after removing.
        UserMetricGroup newCreated =
                metricManager.getOrCreateMetric(
                        metricName,
                        name ->
                                new UserMetricGroup(
                                        NOPMetricRegistry.INSTANCE,
                                        "user_abc",
                                        TABLET_SERVER_METRICS));
        newCreated.incBytesIn(100);

        assertThat(metricManager.getMetric(metricName)).isEqualTo(newCreated);
        assertThat(newCreated.isClosed()).isFalse();
    }
}
