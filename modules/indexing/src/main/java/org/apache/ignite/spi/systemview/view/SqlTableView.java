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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;

/**
 * Sql table representation for a {@link SystemView}.
 */
public class SqlTableView {
    /** Table. */
    private final GridH2Table tbl;

    /** Affinity column name. */
    private String affColName;

    public SqlTableView(GridH2Table tbl) {
        this.tbl = tbl;

        IndexColumn affCol = tbl.getAffinityKeyColumn();

        if (affCol != null) {
            // Only explicit affinity column should be shown. Do not do this for _KEY or it's alias.
            if (!tbl.rowDescriptor().isKeyColumn(affCol.column.getColumnId())) {
                affColName = affCol.columnName;
            }
        }
    }

    /** @return Cache group id. */
    public int cacheGroupId() {
        return tbl.cacheInfo().groupId();
    }

    /** @return Cache group name. */
    public String cacheGroupName() {
        return tbl.cacheInfo().cacheContext().group().cacheOrGroupName();
    }

    /** @return Cache id. */
    public int cacheId() {
        return tbl.cacheId();
    }

    /** @return Cache name. */
    @Order(2)
    public String cacheName() {
        return tbl.cacheName();
    }

    /** @return Schema name. */
    @Order(1)
    public String schemaName() {
        return tbl.getSchema().getName();
    }

    /** @return Table name. */
    @Order
    public String tableName() {
        return tbl.identifier().table();

    }

    /** @return Table identifier as string. */
    public String identifierString() {
        return tbl.identifierString();
    }

    /** @return Affinity key column. */
    public String affinityKeyColumn() {
        return affColName;
    }

    /** @return Key alias. */
    public String keyAlias() {
        return tbl.rowDescriptor().type().keyFieldAlias();
    }

    /** @return Value alias. */
    public String valueAlias() {
        return tbl.rowDescriptor().type().valueFieldAlias();
    }

    /** @return Key type name. */
    public String keyTypeName() {
        return tbl.rowDescriptor().type().keyTypeName();
    }

    /** @return Value type name. */
    public String valueTypeName() {
        return tbl.rowDescriptor().type().valueTypeName();
    }
}
