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
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.ClusterNodeView;

/**
 * Generated by {@code org.apache.ignite.codegen.MonitoringRowAttributeWalkerGenerator}.
 * {@link ClusterNodeView} attributes walker.
 * 
 * @see ClusterNodeView
 */
public class ClusterNodeViewWalker implements MonitoringRowAttributeWalker<ClusterNodeView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "nodeId", UUID.class);
        v.accept(1, "consistentId", String.class);
        v.accept(2, "version", String.class);
        v.acceptBoolean(3, "isClient");
        v.acceptBoolean(4, "isDaemon");
        v.acceptLong(5, "nodeOrder");
        v.accept(6, "addresses", String.class);
        v.accept(7, "hostnames", String.class);
        v.acceptBoolean(8, "isLocal");
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(ClusterNodeView row, AttributeWithValueVisitor v) {
        v.accept(0, "nodeId", UUID.class, row.nodeId());
        v.accept(1, "consistentId", String.class, row.consistentId());
        v.accept(2, "version", String.class, row.version());
        v.acceptBoolean(3, "isClient", row.isClient());
        v.acceptBoolean(4, "isDaemon", row.isDaemon());
        v.acceptLong(5, "nodeOrder", row.nodeOrder());
        v.accept(6, "addresses", String.class, row.addresses());
        v.accept(7, "hostnames", String.class, row.hostnames());
        v.acceptBoolean(8, "isLocal", row.isLocal());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 9;
    }
}

