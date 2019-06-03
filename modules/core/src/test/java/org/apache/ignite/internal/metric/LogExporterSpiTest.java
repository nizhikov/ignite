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

import java.util.Set;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.spi.metric.log.LogExporterSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class LogExporterSpiTest extends AbstractExporterSpiTest {
    /** */
    public static final String FILTERED_PREFIX = "filtered.metric";

    /** */
    public static final int EXPORT_TIMEOUT = 1_000;

    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setGridLogger(log);

        LogExporterSpi logSpi = new LogExporterSpi();

        logSpi.setTimeout(EXPORT_TIMEOUT);

        logSpi.setExportFilter(m -> !m.getName().startsWith(FILTERED_PREFIX));

        cfg.setMetricExporterSpi(logSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testLogSpi() throws Exception {
        cleanPersistenceDir();

        Set<String> expectedAttributes = new GridConcurrentHashSet<>(EXPECTED_ATTRIBUTES);

        log.registerListener(s -> {
            for (String attr : expectedAttributes) {
                if (s.contains(attr))
                    expectedAttributes.remove(attr);
            }
        });

        ignite = startGrid(0);

        boolean res = waitForCondition(expectedAttributes::isEmpty, EXPORT_TIMEOUT * 10);

        assertTrue(res);

        log.registerListener(s -> {
            if(s.contains(FILTERED_PREFIX))
                fail("Filtererd prefix shouldn't export.");
        });

        Set<String> expectedMetrics = new GridConcurrentHashSet<>(asList(
            "other.preifx.test = 42",
            "other.preifx.test2 = 43",
            "other.preifx2.test3 = 44"
        ));

        log.registerListener(s -> {
            for (String metric : expectedMetrics) {
                if (s.contains(metric))
                    expectedAttributes.remove(metric);
            }
        });

        createAdditionalMetrics(ignite);

        res = waitForCondition(expectedMetrics::isEmpty, EXPORT_TIMEOUT * 10);

        assertTrue(res);
    }
}
