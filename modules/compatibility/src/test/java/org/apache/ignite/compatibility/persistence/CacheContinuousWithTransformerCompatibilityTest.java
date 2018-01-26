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

package org.apache.ignite.compatibility.persistence;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TransformedEventListener;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 */
public class CacheContinuousWithTransformerCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "cache1";

    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1, "2.2.0", new IgniteInClosure<IgniteConfiguration>() {
            @Override public void apply(IgniteConfiguration cfg) {
                cfg.setLocalHost("127.0.0.1");

                TcpDiscoverySpi disco = new TcpDiscoverySpi();
                disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

                cfg.setDiscoverySpi(disco);

                cfg.setPeerClassLoadingEnabled(false);
            }
        }, new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite ignite) {
            }
        });

        IgniteEx ignite = startGrid(0);
    }

    @Override protected void afterTestsStopped() throws Exception {
       stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformerNotAvailableBefore2_3() throws Exception {
        IgniteEx ignite = grid(0);

        final IgniteCache<Integer, String> cache = ignite.getOrCreateCache(TEST_CACHE_NAME);

        GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ContinuousQueryWithTransformer<Integer, Integer, String> qry = new ContinuousQueryWithTransformer<>();

                qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(
                    new IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Integer>, String>() {
                    @Override public String apply(CacheEntryEvent<? extends Integer, ? extends Integer> event) {
                        return "" + event.getKey() + " " + event.getValue();
                    }
                }));

                qry.setLocalListener(new TransformedEventListener<String>() {
                    @Override public void onUpdated(Iterable<? extends String> events) {
                        fail("ContinuousQueryWithTransformer can't be run when there is 2.2.0 node in cluster");
                    }
                });

                try (QueryCursor cur = cache.query(qry)) {
                    for(int i=0; i<100; i++)
                        cache.put(i, "" + i);
                }

                return null;
            }
        }, IgniteException.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWorkingWith2_2_and2_3_nodes() throws Exception {
        IgniteEx ignite = grid(0);

        final IgniteCache<Integer, String> cache = ignite.getOrCreateCache(TEST_CACHE_NAME);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final CountDownLatch latch = new CountDownLatch(100);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(
                Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> events) throws CacheEntryListenerException {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> event : events) {
                    latch.countDown();
                }
            }
        });

        try (QueryCursor cur = cache.query(qry)) {
            for(int i=0; i<100; i++)
                cache.put(i, "" + i);

            assertTrue("Listener doesn't receive events", latch.await(30, SECONDS));
        }
    }
}
