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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.TransformedEventListener;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.transactions.Transaction;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheContinuousQueryAsyncFilterListenerTest extends AbstractContinuousQueryTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    public static final int ITERATION_CNT = 100;

    /** */
    private boolean client;

    @Override public boolean isContinuousWithTransformer() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        MemoryEventStorageSpi storeSpi = new MemoryEventStorageSpi();
        storeSpi.setExpireCount(1000);

        cfg.setEventStorageSpi(storeSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    ///
    /// ASYNC FILTER AND LISTENER. TEST LISTENER.
    ///

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerTx() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerTxJCacheApi() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL), true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerAtomic() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerAtomicJCacheApi() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC), true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerReplicatedAtomic() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, ATOMIC), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerReplicatedAtomicJCacheApi() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, ATOMIC), true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerReplicatedAtomicOffHeapValues() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, ATOMIC), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerAtomicWithoutBackup() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 0, ATOMIC), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerAtomicWithoutBackupJCacheApi() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 0, ATOMIC), true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListener() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerReplicated() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, TRANSACTIONAL), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerReplicatedJCacheApi() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, TRANSACTIONAL), true, true, true);
    }

    ///
    /// ASYNC FILTER AND LISTENER. TEST FILTER.
    ///

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTx() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTxJCacheApi() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL), true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomic() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, ATOMIC), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicJCacheApi() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, ATOMIC), true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicatedAtomic() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(REPLICATED, 2, ATOMIC), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicWithoutBackup() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 0, ATOMIC), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilter() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicated() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(REPLICATED, 2, TRANSACTIONAL), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicatedJCacheApi() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(REPLICATED, 2, TRANSACTIONAL), true, true, false);
    }

    ///
    /// ASYNC LISTENER. TEST LISTENER.
    ///

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTxSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL), false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC), false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicatedAtomicSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, ATOMIC), false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicWithoutBackupSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 0, ATOMIC), false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL), false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicatedSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, TRANSACTIONAL), false, true, false);
    }

    /**
     * @param ccfg Cache configuration.
     * @param asyncFltr Async filter.
     * @param asyncLsnr Async listener.
     * @param jcacheApi Use JCache api for registration entry update listener.
     * @throws Exception If failed.
     */
    protected void testNonDeadLockInListener(CacheConfiguration ccfg,
        final boolean asyncFltr,
        boolean asyncLsnr,
        boolean jcacheApi) throws Exception {
        ignite(0).createCache(ccfg);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        try {
            for (int i = 0; i < ITERATION_CNT; i++) {
                log.info("Start iteration: " + i);

                int nodeIdx = i % NODES;

                final String cacheName = ccfg.getName();
                final IgniteCache cache = grid(nodeIdx).cache(cacheName);

                final QueryTestKey key = NODES - 1 != nodeIdx ? affinityKey(cache) : new QueryTestKey(1);

                final QueryTestValue val0 = new QueryTestValue(1);
                final QueryTestValue newVal = new QueryTestValue(2);

                final CountDownLatch latch = new CountDownLatch(1);
                final CountDownLatch evtFromLsnrLatch = new CountDownLatch(1);

                IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> fltrClsr =
                    new IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>>() {
                        @Override public void apply(Ignite ignite, CacheEntryEvent<? extends QueryTestKey,
                            ? extends QueryTestValue> e) {
                            if (asyncFltr) {
                                assertFalse("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("sys-"));

                                assertTrue("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("callback-"));
                            }
                        }
                    };

                CI2<Ignite, T2<QueryTestKey, QueryTestValue>> lsnrClsr =
                    new CI2<Ignite, T2<QueryTestKey, QueryTestValue>>() {
                        @Override public void apply(Ignite ignite, T2<QueryTestKey, QueryTestValue> e) {
                            IgniteCache<Object, Object> cache0 = ignite.cache(cacheName);

                            QueryTestValue val = e.getValue();

                            if (val == null)
                                return;
                            else if (val.equals(newVal)) {
                                evtFromLsnrLatch.countDown();

                                return;
                            }
                            else if (!val.equals(val0))
                                return;

                            Transaction tx = null;

                            try {
                                if (cache0.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL)
                                    tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                                assertEquals(val, val0);

                                cache0.put(key, newVal);

                                if (tx != null)
                                    tx.commit();

                                latch.countDown();
                            }
                            catch (Exception exp) {
                                log.error("Failed: ", exp);

                                throw new IgniteException(exp);
                            }
                            finally {
                                if (tx != null)
                                    tx.close();
                            }
                        }
                    };

                QueryCursor qry = null;
                MutableCacheEntryListenerConfiguration<QueryTestKey, QueryTestValue> lsnrCfg = null;

                CacheInvokeListener<QueryTestKey, QueryTestValue> locLsnr =
                    asyncLsnr ? new CacheInvokeListenerAsync<>(lsnrClsr) : new CacheInvokeListener<>(lsnrClsr);

                CacheEntryEventSerializableFilter<QueryTestKey, QueryTestValue> rmtFltr =
                    asyncFltr ? new CacheTestRemoteFilterAsync(fltrClsr) : new CacheTestRemoteFilter(fltrClsr);

                if (jcacheApi) {
                    lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
                        FactoryBuilder.factoryOf(locLsnr),
                        FactoryBuilder.factoryOf(rmtFltr),
                        true,
                        false
                    );

                    cache.registerCacheEntryListener(lsnrCfg);
                }
                else {
                    Query conQry = createContinuousQuery();

                    if (isContinuousWithTransformer()) {
                        TransformedEventListener locTransLsnr = asyncLsnr ?
                            new TransformedEventListenerImplAsync(lsnrClsr) :
                            new TransformedEventListenerImpl(lsnrClsr);
                        ((ContinuousQueryWithTransformer)conQry).setLocalTransformedEventListener(locTransLsnr);
                    } else
                        ((ContinuousQuery)conQry).setLocalListener(locLsnr);

                    setRemoteFilterFactory(conQry, FactoryBuilder.factoryOf(rmtFltr));

                    qry = cache.query(conQry);
                }

                try {
                    if (rnd.nextBoolean())
                        cache.put(key, val0);
                    else {
                        cache.invoke(key, new CacheEntryProcessor() {
                            @Override public Object process(MutableEntry entry, Object... arguments)
                                throws EntryProcessorException {
                                entry.setValue(val0);

                                return null;
                            }
                        });
                    }

                    assertTrue("Failed to waiting event.", U.await(latch, 3, SECONDS));

                    assertEquals(cache.get(key), new QueryTestValue(2));

                    assertTrue("Failed to waiting event from listener.", U.await(latch, 3, SECONDS));
                }
                finally {
                    if (qry != null)
                        qry.close();

                    if (lsnrCfg != null)
                        cache.deregisterCacheEntryListener(lsnrCfg);
                }

                log.info("Iteration finished: " + i);
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @param asyncFilter Async filter.
     * @param asyncLsnr Async listener.
     * @param jcacheApi Use JCache api for start update listener.
     * @throws Exception If failed.
     */
    private void testNonDeadLockInFilter(CacheConfiguration ccfg,
        final boolean asyncFilter,
        final boolean asyncLsnr,
        boolean jcacheApi) throws Exception {
        ignite(0).createCache(ccfg);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        try {
            for (int i = 0; i < ITERATION_CNT; i++) {
                log.info("Start iteration: " + i);

                int nodeIdx = i % NODES;

                final String cacheName = ccfg.getName();
                final IgniteCache cache = grid(nodeIdx).cache(cacheName);

                final QueryTestKey key = NODES - 1 != nodeIdx ? affinityKey(cache) : new QueryTestKey(1);

                final QueryTestValue val0 = new QueryTestValue(1);
                final QueryTestValue newVal = new QueryTestValue(2);

                final CountDownLatch latch = new CountDownLatch(1);
                final CountDownLatch evtFromLsnrLatch = new CountDownLatch(1);

                IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> fltrClsr =
                    new IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>>() {
                        @Override public void apply(Ignite ignite, CacheEntryEvent<? extends QueryTestKey,
                            ? extends QueryTestValue> e) {
                            if (asyncFilter) {
                                assertFalse("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("sys-"));

                                assertTrue("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("callback-"));
                            }

                            IgniteCache<Object, Object> cache0 = ignite.cache(cacheName);

                            QueryTestValue val = e.getValue();

                            if (val == null)
                                return;
                            else if (val.equals(newVal)) {
                                evtFromLsnrLatch.countDown();

                                return;
                            }
                            else if (!val.equals(val0))
                                return;

                            Transaction tx = null;

                            try {
                                if (cache0.getConfiguration(CacheConfiguration.class)
                                    .getAtomicityMode() == TRANSACTIONAL)
                                    tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                                assertEquals(val, val0);

                                cache0.put(key, newVal);

                                if (tx != null)
                                    tx.commit();

                                latch.countDown();
                            }
                            catch (Exception exp) {
                                log.error("Failed: ", exp);

                                throw new IgniteException(exp);
                            }
                            finally {
                                if (tx != null)
                                    tx.close();
                            }
                        }
                    };

                CI2<Ignite, T2<QueryTestKey, QueryTestValue>> lsnrClsr =
                    new CI2<Ignite, T2<QueryTestKey, QueryTestValue>>() {
                        @Override public void apply(Ignite ignite, T2<QueryTestKey, QueryTestValue> e) {
                            if (asyncLsnr) {
                                assertFalse("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("sys-"));

                                assertTrue("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("callback-"));
                            }

                            QueryTestValue val = e.getValue();

                            if (val == null || !val.equals(new QueryTestValue(1)))
                                return;

                            assertEquals(val, val0);

                            latch.countDown();
                        }
                    };


                QueryCursor qry = null;
                MutableCacheEntryListenerConfiguration<QueryTestKey, QueryTestValue> lsnrCfg = null;

                CacheInvokeListener<QueryTestKey, QueryTestValue> locLsnr =
                    asyncLsnr ? new CacheInvokeListenerAsync<>(lsnrClsr) : new CacheInvokeListener<>(lsnrClsr);

                CacheEntryEventSerializableFilter<QueryTestKey, QueryTestValue> rmtFltr = asyncFilter ?
                    new CacheTestRemoteFilterAsync(fltrClsr) : new CacheTestRemoteFilter(fltrClsr);

                if (jcacheApi) {
                    lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
                        FactoryBuilder.factoryOf(locLsnr),
                        FactoryBuilder.factoryOf(rmtFltr),
                        true,
                        false
                    );

                    cache.registerCacheEntryListener(lsnrCfg);
                }
                else {
                    Query conQry = createContinuousQuery();

                    if (isContinuousWithTransformer()) {
                        TransformedEventListener locTransLsnr = asyncLsnr ?
                            new TransformedEventListenerImplAsync(lsnrClsr) :
                            new TransformedEventListenerImpl(lsnrClsr);
                        ((ContinuousQueryWithTransformer)conQry).setLocalTransformedEventListener(locTransLsnr);
                    } else
                        ((ContinuousQuery)conQry).setLocalListener(locLsnr);

                    setRemoteFilterFactory(conQry, FactoryBuilder.factoryOf(rmtFltr));

                    qry = cache.query(conQry);
                }

                try {
                    if (rnd.nextBoolean())
                        cache.put(key, val0);
                    else
                        cache.invoke(key, new CacheEntryProcessor() {
                            @Override public Object process(MutableEntry entry, Object... arguments)
                                throws EntryProcessorException {
                                entry.setValue(val0);

                                return null;
                            }
                        });

                    assert U.await(latch, 3, SECONDS) : "Failed to waiting event.";

                    assertEquals(new QueryTestValue(2), cache.get(key));

                    assertTrue("Failed to waiting event from filter.", U.await(latch, 3, SECONDS));
                }
                finally {
                    if (qry != null)
                        qry.close();

                    if (lsnrCfg != null)
                        cache.deregisterCacheEntryListener(lsnrCfg);
                }

                log.info("Iteration finished: " + i);
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Ignite cache.
     * @return Key.
     */
    private QueryTestKey affinityKey(IgniteCache cache) {
        Affinity aff = affinity(cache);

        for (int i = 0; i < 10_000; i++) {
            QueryTestKey key = new QueryTestKey(i);

            if (aff.isPrimary(localNode(cache), key))
                return key;
        }

        throw new IgniteException("Failed to found primary key.");
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.SECONDS.toMillis(2 * 60);
    }


    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setName("test-cache-" + atomicityMode + "-" + cacheMode + "-" + backups);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    public static class QueryTestKey implements Serializable, Comparable {
        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public QueryTestKey(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestKey that = (QueryTestKey)o;

            return key.equals(that.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestKey.class, this);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(Object o) {
            return key - ((QueryTestKey)o).key;
        }
    }

    /**
     *
     */
    public static class QueryTestValue implements Serializable {
        /** */
        @GridToStringInclude
        protected final Integer val1;

        /** */
        @GridToStringInclude
        protected final String val2;

        /**
         * @param val Value.
         */
        public QueryTestValue(Integer val) {
            this.val1 = val;
            this.val2 = String.valueOf(val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestValue that = (QueryTestValue)o;

            return val1.equals(that.val1) && val2.equals(that.val2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val1.hashCode();

            res = 31 * res + val2.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestValue.class, this);
        }
    }
}
