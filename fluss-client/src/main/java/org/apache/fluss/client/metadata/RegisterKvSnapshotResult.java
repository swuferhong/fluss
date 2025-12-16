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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import java.util.Set;

/**
 * A class to represent the result of registering kv snapshot. It contains:
 *
 * <ul>
 *   <li>An set of failed tableBuckets. Such as the specify snapshotId is not exist for this table
 *       bucket.
 * </ul>
 *
 * @since 0.9
 */
@PublicEvolving
public class RegisterKvSnapshotResult {
    private final Set<TableBucket> failedTableBucketSet;

    public RegisterKvSnapshotResult(Set<TableBucket> failedTableBucketSet) {
        this.failedTableBucketSet = failedTableBucketSet;
    }

    public Set<TableBucket> getFailedTableBucketSet() {
        return failedTableBucketSet;
    }
}
