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

package org.apache.ignite.internal.processors.query;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.metric.list.MonitoringListExclude;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;

/**
 * Query descriptor.
 */
public class GridRunningQueryInfo implements MonitoringRow<Long> {
    /** */
    private final long id;

    /** Originating Node ID. */
    private final UUID nodeId;

    /** */
    private final String qry;

    /** Query type. */
    private final GridCacheQueryType qryType;

    /** Schema name. */
    private final String schemaName;

    /** */
    private final long startTime;

    /** */
    private final GridQueryCancel cancel;

    /** */
    private final boolean loc;

    /** */
    private final QueryRunningFuture fut = new QueryRunningFuture();

    /** */
    private boolean failed;

    /**
     * Constructor.
     *
     * @param id Query ID.
     * @param nodeId Originating node ID.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param startTime Query start time.
     * @param cancel Query cancel.
     * @param loc Local query flag.
     */
    public GridRunningQueryInfo(
        Long id,
        UUID nodeId,
        String qry,
        GridCacheQueryType qryType,
        String schemaName,
        long startTime,
        GridQueryCancel cancel,
        boolean loc
    ) {
        this.id = id;
        this.nodeId = nodeId;
        this.qry = qry;
        this.qryType = qryType;
        this.schemaName = schemaName;
        this.startTime = startTime;
        this.cancel = cancel;
        this.loc = loc;
    }

    /** {@inheritDoc} */
    @Override public String sessionId() {
        return nodeId.toString();
    }

    /**
     * @return Query ID.
     */
    public Long id() {
        return id;
    }

    /**
     * @return Global query ID.
     */
    public String globalQueryId() {
        return QueryUtils.globalQueryId(nodeId, id);
    }

    /**
     * @return Query text.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType queryType() {
        return qryType;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Query start time.
     */
    public long startTime() {
        return startTime;
    }

    /**
     * @param curTime Current time.
     * @param duration Duration of long query.
     * @return {@code true} if this query should be considered as long running query.
     */
    public boolean longQuery(long curTime, long duration) {
        return curTime - startTime > duration;
    }

    /**
     * Cancel query.
     */
    public void cancel() {
        if (cancel != null)
            cancel.cancel();
    }

    /**
     * @return Query running future.
     */
    @MonitoringListExclude public QueryRunningFuture runningFuture() {
        return fut;
    }

    /**
     * @return {@code true} if query can be cancelled.
     */
    public boolean cancelable() {
        return cancel != null;
    }

    /**
     * @return {@code true} if query is local.
     */
    public boolean local() {
        return loc;
    }

    /** */
    public void failed(boolean failed) {
        this.failed = failed;
    }

    /** */
    public boolean failed() { return failed; }
}
