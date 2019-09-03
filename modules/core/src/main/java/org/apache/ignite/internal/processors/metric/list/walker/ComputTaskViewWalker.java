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

import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.ComputTaskView;

/** */
public class ComputTaskViewWalker implements MonitoringRowAttributeWalker<ComputTaskView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "affinityCacheName", String.class);
        v.acceptInt(1, "affinityPartitionId");
        v.acceptLong(2, "endTime");
        v.accept(3, "execName", String.class);
        v.acceptBoolean(4, "internal");
        v.accept(5, "jobId", IgniteUuid.class);
        v.acceptLong(6, "startTime");
        v.accept(7, "taskClassName", String.class);
        v.accept(8, "taskName", String.class);
        v.accept(9, "taskNodeId", UUID.class);
        v.accept(10, "userVersion", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(ComputTaskView row, AttributeWithValueVisitor v) {
        v.accept(0, "affinityCacheName", String.class, row.affinityCacheName());
        v.acceptInt(1, "affinityPartitionId", row.affinityPartitionId());
        v.acceptLong(2, "endTime", row.endTime());
        v.accept(3, "execName", String.class, row.execName());
        v.acceptBoolean(4, "internal", row.internal());
        v.accept(5, "jobId", IgniteUuid.class, row.jobId());
        v.acceptLong(6, "startTime", row.startTime());
        v.accept(7, "taskClassName", String.class, row.taskClassName());
        v.accept(8, "taskName", String.class, row.taskName());
        v.accept(9, "taskNodeId", UUID.class, row.taskNodeId());
        v.accept(10, "userVersion", String.class, row.userVersion());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 11;
    }
}

