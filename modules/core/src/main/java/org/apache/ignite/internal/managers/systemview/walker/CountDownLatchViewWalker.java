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

package org.apache.ignite.internal.managers.systemview.walker;

import org.apache.ignite.spi.systemview.view.datastructures.CountDownLatchView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link CountDownLatchView} attributes walker.
 * 
 * @see CountDownLatchView
 */
public class CountDownLatchViewWalker implements SystemViewRowAttributeWalker<CountDownLatchView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "name", String.class);
        v.accept(1, "count", int.class);
        v.accept(2, "initialCount", int.class);
        v.accept(3, "autoDelete", boolean.class);
        v.accept(4, "groupName", String.class);
        v.accept(5, "groupId", int.class);
        v.accept(6, "removed", boolean.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(CountDownLatchView row, AttributeWithValueVisitor v) {
        v.accept(0, "name", String.class, row.name());
        v.acceptInt(1, "count", row.count());
        v.acceptInt(2, "initialCount", row.initialCount());
        v.acceptBoolean(3, "autoDelete", row.autoDelete());
        v.accept(4, "groupName", String.class, row.groupName());
        v.acceptInt(5, "groupId", row.groupId());
        v.acceptBoolean(6, "removed", row.removed());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 7;
    }
}
