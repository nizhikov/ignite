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

import org.apache.ignite.spi.systemview.view.SqlViewColumnView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link SqlViewColumnView} attributes walker.
 * 
 * @see SqlViewColumnView
 */
public class SqlViewColumnViewWalker implements SystemViewRowAttributeWalker<SqlViewColumnView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "columnName", String.class);
        v.accept(1, "viewName", String.class);
        v.accept(2, "schemaName", String.class);
        v.accept(3, "defatult", String.class);
        v.accept(4, "nullable", boolean.class);
        v.accept(5, "precision", long.class);
        v.accept(6, "scale", int.class);
        v.accept(7, "type", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(SqlViewColumnView row, AttributeWithValueVisitor v) {
        v.accept(0, "columnName", String.class, row.columnName());
        v.accept(1, "viewName", String.class, row.viewName());
        v.accept(2, "schemaName", String.class, row.schemaName());
        v.accept(3, "defatult", String.class, row.defatult());
        v.acceptBoolean(4, "nullable", row.nullable());
        v.acceptLong(5, "precision", row.precision());
        v.acceptInt(6, "scale", row.scale());
        v.accept(7, "type", String.class, row.type());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 8;
    }
}
