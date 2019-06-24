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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Adapter for {@link QueryMetrics}.
 */
public class GridCacheQueryMetricsAdapter implements QueryMetrics {
    /** Minimum time of execution. */
    private final LongMetricImpl minTime;

    /** Maximum time of execution. */
    private final LongMetricImpl maxTime;

    /** Sum of execution time for all completed queries. */
    private final LongAdderMetricImpl sumTime;

    /** Number of executions. */
    private final LongAdderMetricImpl execs;

    /** Number of completed executions. */
    private final LongAdderMetricImpl completed;

    /** Number of fails. */
    private final LongAdderMetricImpl fails;

    /**
     * @param mreg Metric registry.
     * @param cacheName Cache name.
     */
    public GridCacheQueryMetricsAdapter(MetricRegistry mreg, String cacheName, @Nullable String suffix) {
        String prefix;

        if (suffix == null)
            prefix = metricName(CACHE_METRICS_PREFIX, cacheName, "query");
        else
            prefix = metricName(CACHE_METRICS_PREFIX, cacheName, suffix, "query");

        mreg = mreg.withPrefix(prefix);

        minTime = mreg.metric("MinimalTime", null);
        minTime.value(Long.MAX_VALUE);

        maxTime = mreg.metric("MaximumTime", null);
        sumTime = mreg.longAdderMetric("SumTime", null);
        execs = mreg.longAdderMetric("Executed", null);
        completed = mreg.longAdderMetric("Completed", null);
        fails = mreg.longAdderMetric("Failed", null);
    }

    /** {@inheritDoc} */
    @Override public long minimumTime() {
        long min = minTime.get();

        return min == Long.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public long maximumTime() {
        return maxTime.get();
    }

    /** {@inheritDoc} */
    @Override public double averageTime() {
        double val = completed.longValue();

        return val > 0 ? sumTime.longValue() / val : 0.0;
    }

    /** {@inheritDoc} */
    @Override public int executions() {
        return (int)execs.longValue();
    }

    /** {@inheritDoc} */
    @Override public int fails() {
        return (int)fails.longValue();
    }

    /**
     * Update metrics.
     *
     * @param duration Duration of queue execution.
     * @param fail {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public void update(long duration, boolean fail) {
        if (fail) {
            execs.increment();
            fails.increment();
        }
        else {
            execs.increment();
            completed.increment();

            MetricUtils.setIfLess(minTime, duration);
            MetricUtils.setIfGreater(maxTime, duration);

            sumTime.add(duration);
        }
    }

    /**
     * Merge with given metrics.
     *
     * @return Copy.
     */
    public QueryMetrics snapshot() {
        long minTimeVal = minTime.longValue();

        return new QueryMetricsSnapshot(
            minTimeVal == Long.MAX_VALUE ? 0 : minTimeVal,
            maxTime.longValue(),
            averageTime(),
            (int)execs.longValue(),
            (int)fails.longValue());
    }

    /**
     * Resets query metrics.
     */
    public void reset() {
        minTime.value(Long.MAX_VALUE);
        maxTime.reset();
        sumTime.reset();
        execs.reset();
        completed.reset();
        fails.reset();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryMetricsAdapter.class, this);
    }

    /**
     * Query metrics snapshot.
     */
    public static class QueryMetricsSnapshot implements QueryMetrics, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private long minTime;

        /** */
        private long maxTime;

        /** */
        private double avgTime;

        /** */
        private int execs;

        /** */
        private int fails;

        /** */
        public QueryMetricsSnapshot(long minTime, long maxTime, double avgTime, int execs, int fails) {
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.avgTime = avgTime;
            this.execs = execs;
            this.fails = fails;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(minTime);
            out.writeLong(maxTime);
            out.writeDouble(avgTime);
            out.writeInt(execs);
            out.writeInt(fails);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            minTime = in.readLong();
            maxTime = in.readLong();
            avgTime = in.readDouble();
            execs = in.readInt();
            fails = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public long minimumTime() {
            return minTime;
        }

        /** {@inheritDoc} */
        @Override public long maximumTime() {
            return maxTime;
        }

        /** {@inheritDoc} */
        @Override public double averageTime() {
            return avgTime;
        }

        /** {@inheritDoc} */
        @Override public int executions() {
            return execs;
        }

        /** {@inheritDoc} */
        @Override public int fails() {
            return fails;
        }
    }
}
