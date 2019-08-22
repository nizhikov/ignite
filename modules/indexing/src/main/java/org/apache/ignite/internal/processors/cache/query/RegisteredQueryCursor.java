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
 *
 */

package org.apache.ignite.internal.processors.cache.query;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.metric.list.QueryView;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.unregister;

/**
 * Query cursor for registered as running queries.
 *
 * Running query will be unregistered during close of cursor.
 */
public class RegisteredQueryCursor<T> extends QueryCursorImpl<T> {
    /** */
    private final AtomicBoolean unregistered = new AtomicBoolean(false);

    /** */
    private MonitoringList<Long, QueryView> sqlQryMonList;

    /** */
    private long qryId;

    /** Flag to indicate error. */
    private boolean failed;

    /**
     * @param iterExec Query executor.
     * @param cancel Cancellation closure.
     * @param sqlQryMonList Query monitoring list.
     * @param qryId Registered running query id.
     */
    public RegisteredQueryCursor(Iterable<T> iterExec, GridQueryCancel cancel,
        MonitoringList<Long, QueryView> sqlQryMonList, long qryId) {
        super(iterExec, cancel);

        assert sqlQryMonList != null;

        this.sqlQryMonList = sqlQryMonList;
        this.qryId = qryId;
    }

    @Override protected Iterator<T> iter() {
        try {
            return super.iter();
        }
        catch (Exception e) {
            failed = true;

            if (X.cause(e, QueryCancelledException.class) != null)
                unregisterQuery();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        unregisterQuery();

        super.close();
    }

    /**
     * Unregister query.
     */
    private void unregisterQuery(){
        if (unregistered.compareAndSet(false, true))
            unregister(sqlQryMonList, qryId, failed);
    }
}
