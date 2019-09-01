/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.metric.list.walker;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.CacheGroupView;

/** */
public class CacheGroupViewWalker implements MonitoringRowAttributeWalker<CacheGroupView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "cacheGroupName", String.class);
        v.acceptInt(1, "cacheCount");
        v.accept(2, "dataRegionName", String.class);
        v.accept(3, "cacheMode", CacheMode.class);
        v.accept(4, "atomicityMode", CacheAtomicityMode.class);
        v.accept(5, "affinity", String.class);
        v.accept(6, "backups", Integer.class);
        v.acceptInt(7, "cacheGroupId");
        v.acceptBoolean(8, "isShared");
        v.accept(9, "nodeFilter", String.class);
        v.accept(10, "partitionLossPolicy", PartitionLossPolicy.class);
        v.accept(11, "partitionsCount", Integer.class);
        v.acceptLong(12, "rebalanceDelay");
        v.accept(13, "rebalanceMode", CacheRebalanceMode.class);
        v.acceptInt(14, "rebalanceOrder");
        v.accept(15, "topologyValidator", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(CacheGroupView row, AttributeWithValueVisitor v) {
        v.accept(0, "cacheGroupName", String.class, row.cacheGroupName());
        v.acceptInt(1, "cacheCount", row.cacheCount());
        v.accept(2, "dataRegionName", String.class, row.dataRegionName());
        v.accept(3, "cacheMode", CacheMode.class, row.cacheMode());
        v.accept(4, "atomicityMode", CacheAtomicityMode.class, row.atomicityMode());
        v.accept(5, "affinity", String.class, row.affinity());
        v.accept(6, "backups", Integer.class, row.backups());
        v.acceptInt(7, "cacheGroupId", row.cacheGroupId());
        v.acceptBoolean(8, "isShared", row.isShared());
        v.accept(9, "nodeFilter", String.class, row.nodeFilter());
        v.accept(10, "partitionLossPolicy", PartitionLossPolicy.class, row.partitionLossPolicy());
        v.accept(11, "partitionsCount", Integer.class, row.partitionsCount());
        v.acceptLong(12, "rebalanceDelay", row.rebalanceDelay());
        v.accept(13, "rebalanceMode", CacheRebalanceMode.class, row.rebalanceMode());
        v.acceptInt(14, "rebalanceOrder", row.rebalanceOrder());
        v.accept(15, "topologyValidator", String.class, row.topologyValidator());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 16;
    }
}

