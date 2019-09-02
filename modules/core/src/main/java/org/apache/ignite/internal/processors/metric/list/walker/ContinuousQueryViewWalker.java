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
import org.apache.ignite.spi.metric.list.view.ContinuousQueryView;

/** */
public class ContinuousQueryViewWalker implements MonitoringRowAttributeWalker<ContinuousQueryView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "cacheName", String.class);
        v.accept(1, "localListener", String.class);
        v.accept(2, "remoteFilter", String.class);
        v.accept(3, "remoteTransformer", String.class);
        v.accept(4, "localTransformedListener", String.class);
        v.acceptLong(5, "lastSendTime");
        v.acceptBoolean(6, "autoUnsubscribe");
        v.acceptInt(7, "bufferSize");
        v.acceptBoolean(8, "delayedRegister");
        v.acceptLong(9, "interval");
        v.acceptBoolean(10, "isEvents");
        v.acceptBoolean(11, "isMessaging");
        v.acceptBoolean(12, "isQuery");
        v.acceptBoolean(13, "keepBinary");
        v.accept(14, "monitoringRowId", Object.class);
        v.accept(15, "monitoringRowId", UUID.class);
        v.accept(16, "nodeId", UUID.class);
        v.acceptBoolean(17, "notifyExisting");
        v.acceptBoolean(18, "oldValueRequired");
        v.accept(19, "routineId", UUID.class);
        v.accept(20, "topic", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(ContinuousQueryView row, AttributeWithValueVisitor v) {
        v.accept(0, "cacheName", String.class, row.cacheName());
        v.accept(1, "localListener", String.class, row.localListener());
        v.accept(2, "remoteFilter", String.class, row.remoteFilter());
        v.accept(3, "remoteTransformer", String.class, row.remoteTransformer());
        v.accept(4, "localTransformedListener", String.class, row.localTransformedListener());
        v.acceptLong(5, "lastSendTime", row.lastSendTime());
        v.acceptBoolean(6, "autoUnsubscribe", row.autoUnsubscribe());
        v.acceptInt(7, "bufferSize", row.bufferSize());
        v.acceptBoolean(8, "delayedRegister", row.delayedRegister());
        v.acceptLong(9, "interval", row.interval());
        v.acceptBoolean(10, "isEvents", row.isEvents());
        v.acceptBoolean(11, "isMessaging", row.isMessaging());
        v.acceptBoolean(12, "isQuery", row.isQuery());
        v.acceptBoolean(13, "keepBinary", row.keepBinary());
        v.accept(14, "monitoringRowId", Object.class, row.monitoringRowId());
        v.accept(15, "monitoringRowId", UUID.class, row.monitoringRowId());
        v.accept(16, "nodeId", UUID.class, row.nodeId());
        v.acceptBoolean(17, "notifyExisting", row.notifyExisting());
        v.acceptBoolean(18, "oldValueRequired", row.oldValueRequired());
        v.accept(19, "routineId", UUID.class, row.routineId());
        v.accept(20, "topic", String.class, row.topic());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 21;
    }
}

