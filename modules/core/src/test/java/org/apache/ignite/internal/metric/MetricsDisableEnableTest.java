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

package org.apache.ignite.internal.metric;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongGauge;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.UP_TIME;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/** */
public class MetricsDisableEnableTest extends GridCommonAbstractTest {
    /** */
    public static final String DEFAULT_CACHE_REGISTRY = cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false);

    /** */
    public static final String SOME_OTHER_CACHE = "some-other-cache";

    /** */
    public static final String SOME_CACHE = "some-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg;
    }

    /** */
    @Test
    public void testDisabledMetrics() throws Exception {
        IgniteEx g = startGrid();

        IgniteMXBean bean = (IgniteMXBean)g;

        bean.disableMetricRegistry(DEFAULT_CACHE_REGISTRY);

        assertTrue("Default cache registry should be disabled.",
            g.context().metric().registry(DEFAULT_CACHE_REGISTRY).disabled());

        g.createCache(SOME_CACHE);

        assertFalse("\"some-cache\" registry should be enabled.",
            g.context().metric().registry(cacheMetricsRegistryName(SOME_CACHE, false)).disabled());

        bean.disableMetricRegistry(SYS_METRICS);

        MetricRegistry mreg = g.context().metric().registry(SYS_METRICS);

        assertTrue("Sys registry should be disabled.", mreg.disabled());

        LongGauge upTime = (LongGauge)mreg.findMetric(UP_TIME);

        assertEquals("Disabled metric should always return 0", 0, upTime.value());

        bean.enableMetricRegistry(SYS_METRICS);

        assertTrue("Enabled metric should return its value", upTime.value() > 0);

        bean.disableMetricRegistry(SYS_METRICS);

        assertEquals("Disabled metric should always return 0", 0, upTime.value());

        bean.disableMetricRegistry(cacheMetricsRegistryName(SOME_OTHER_CACHE, false));

        g.createCache(SOME_OTHER_CACHE);

        assertTrue("\"some-other-cache\" registry disabled in runtime.",
            g.context().metric().registry(cacheMetricsRegistryName(SOME_OTHER_CACHE, false)).disabled());

        g.destroyCache(SOME_OTHER_CACHE);

        bean.enableMetricRegistry(cacheMetricsRegistryName(SOME_OTHER_CACHE, false));

        g.createCache(SOME_OTHER_CACHE);

        assertFalse("\"some-other-cache\" registry enabled in runtime.",
            g.context().metric().registry(cacheMetricsRegistryName(SOME_OTHER_CACHE, false)).disabled());
    }
}
