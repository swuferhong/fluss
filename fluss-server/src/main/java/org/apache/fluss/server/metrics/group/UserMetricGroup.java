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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;

import java.util.Map;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** Metrics for the users in server with {@link TabletServerMetricGroup} as parent group. */
public class UserMetricGroup extends AbstractMetricGroup {
    private static final String NAME = "user";

    private final String principalName;
    protected final Counter bytesIn;
    protected final Counter bytesOut;

    public UserMetricGroup(
            MetricRegistry registry,
            String principalName,
            TabletServerMetricGroup tabletServerMetricGroup) {
        super(registry, makeScope(tabletServerMetricGroup, principalName), tabletServerMetricGroup);
        this.principalName = principalName;

        bytesIn = new ThreadSafeSimpleCounter();
        bytesOut = new ThreadSafeSimpleCounter();
        counter(MetricNames.BYTES_IN_COUNT, bytesIn);
        meter(MetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
        counter(MetricNames.BYTES_OUT_COUNT, bytesOut);
        meter(MetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));
    }

    @VisibleForTesting
    public String getPrincipalName() {
        return principalName;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put("user", principalName);
    }

    public void incBytesIn(long n) {
        this.lastRecordTime = System.currentTimeMillis();
        bytesIn.inc(n);
    }

    public void incBytesOut(long n) {
        this.lastRecordTime = System.currentTimeMillis();
        bytesOut.inc(n);
    }
}
