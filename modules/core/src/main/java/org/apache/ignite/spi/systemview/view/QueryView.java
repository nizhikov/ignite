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
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Query representation for a {@link SystemView}.
 */
public class QueryView {
    /** */
    private String qry;

    /** */
    private String cacheName;

    /** */
    private GridCacheQueryType type;

    /** */
    private long startTime;

    /**
     * @param qry
     * @param cacheName
     * @param type
     * @param startTime
     */
    public QueryView(String qry, String cacheName, GridCacheQueryType type, long startTime) {
        this.qry = qry;
        this.cacheName = cacheName;
        this.type = type;
        this.startTime = startTime;
    }

    /** @return Cache name. */
    @Order
    public String cacheName() {
        return cacheName;
    }

    /** @return Query text. */
    @Order(1)
    public String query() {
        return qry;
    }

    /** @return Query type. */
    @Order(2)
    public GridCacheQueryType type() {
        return type;
    }

    /** @return Start time. */
    @Order(3)
    public long startTime() {
        return startTime;
    }

    /** @return Query duration. */
    @Order(4)
    public long duration() {
        return U.currentTimeMillis() - startTime;
    }
}
