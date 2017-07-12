package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.TransformedEventListener;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class SimpleCacheContinuousWithTransformerTest extends GridCommonAbstractTest {
    private static final int DFLT_ENTRY_CNT = 10;

    private static final int DFLT_LATCH_TIMEOUT = 30_000;

    private static final int DFLT_SERVER_NODE_CNT = 1;

    private static final String SARAH_CONNOR = "Sarah Connor";

    private static final String JOHN_CONNOR = "John Connor";

    private static final boolean ADD_REG_LOC_LSNR = true;

    private static final boolean SKIP_REG_LOC_LSNR = false;

    private static final boolean ADD_EVT_FILTER = true;

    private static final boolean SKIP_EVT_FILTER = false;

    private static final boolean KEEP_BINARY = true;

    private static final boolean SKIP_KEEP_BINARY = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    private IgniteConfiguration getClientConfiguration() throws Exception {
        IgniteConfiguration cfg = getConfiguration("client");
        cfg.setClientMode(true);
        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformer() throws Exception {
        runContinuousQueryWithTransformer(SKIP_REG_LOC_LSNR, SKIP_EVT_FILTER, DFLT_ENTRY_CNT, DFLT_ENTRY_CNT,
            SKIP_KEEP_BINARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformerAndRegularListener() throws Exception {
        runContinuousQueryWithTransformer(ADD_REG_LOC_LSNR, SKIP_EVT_FILTER, DFLT_ENTRY_CNT, DFLT_ENTRY_CNT,
            SKIP_KEEP_BINARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformerWithFilter() throws Exception {
        runContinuousQueryWithTransformer(SKIP_REG_LOC_LSNR, ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2,
            DFLT_ENTRY_CNT / 2, SKIP_KEEP_BINARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformerAndRegularListenerWithFilter() throws Exception {
        runContinuousQueryWithTransformer(ADD_REG_LOC_LSNR, ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2,
            DFLT_ENTRY_CNT / 2, KEEP_BINARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformerKeepBinary() throws Exception {
        runContinuousQueryWithTransformer(SKIP_REG_LOC_LSNR, SKIP_EVT_FILTER, DFLT_ENTRY_CNT, DFLT_ENTRY_CNT,
            KEEP_BINARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformerAndRegularListenerKeepBinary() throws Exception {
        runContinuousQueryWithTransformer(ADD_REG_LOC_LSNR, SKIP_EVT_FILTER, DFLT_ENTRY_CNT, DFLT_ENTRY_CNT,
            KEEP_BINARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformerWithFilterKeepBinary() throws Exception {
        runContinuousQueryWithTransformer(SKIP_REG_LOC_LSNR, ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2,
            DFLT_ENTRY_CNT / 2, KEEP_BINARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousWithTransformerAndRegularListenerWithFilterKeepBinary() throws Exception {
        runContinuousQueryWithTransformer(ADD_REG_LOC_LSNR, ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2,
            DFLT_ENTRY_CNT / 2, KEEP_BINARY);
    }

    private void runContinuousQueryWithTransformer(boolean addRegLocLsnr, boolean addEvtFilter, int expTransCnt,
        int expRegCnt, boolean keepBinary) throws Exception {
        try {
            startGrids(DFLT_SERVER_NODE_CNT);

            Ignite ignite = startGrid(getClientConfiguration());

            IgniteCache<Integer, Employee> cache = ignite.cache(DEFAULT_CACHE_NAME);
            if (keepBinary) {
                cache = cache.withKeepBinary();
            }

            populateData(cache, JOHN_CONNOR);

            CountDownLatch transUpdCnt = new CountDownLatch(expTransCnt);
            AtomicInteger transCnt = new AtomicInteger(0);

            CountDownLatch regUpdCount = new CountDownLatch(expRegCnt);
            AtomicInteger regCnt = new AtomicInteger(0);

            TransformedEventListener<String> transLsnr = new LocalTransformedEventListener(transCnt, transUpdCnt);

            CacheEntryUpdatedListener lsnr = null;
            if (addRegLocLsnr) {
                lsnr = new LocalCacheEntryUpdatedListener(regCnt, regUpdCount, keepBinary);
            }

            Factory<? extends CacheEntryEventFilter> rmtFilterFactory = null;
            if (addEvtFilter) {
                rmtFilterFactory = FactoryBuilder.factoryOf(new RemoteCacheEntryEventFilter());
            }

            ContinuousQueryWithTransformer<Integer, Employee, String> qry = new ContinuousQueryWithTransformer<>();

            qry.setInitialQuery(new ScanQuery<Integer, Employee>());
            qry.setLocalListener(lsnr);
            qry.setRemoteFilterFactory((Factory<? extends CacheEntryEventFilter<Integer, Employee>>)rmtFilterFactory);
            qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(new RemoteTransformer()));
            qry.setLocalTransformedEventListener(transLsnr);

            try (QueryCursor<Cache.Entry<Integer, Employee>> cur = cache.query(qry)) {
                for (Cache.Entry<Integer, Employee> e : cur) {
                    assertNotNull(e);
                }

                populateData(cache, SARAH_CONNOR);

                if (addRegLocLsnr) {
                    assertTrue("Receive all expected events in regular listener",
                        regUpdCount.await(DFLT_LATCH_TIMEOUT, TimeUnit.MILLISECONDS));
                    assertEquals("Count of updated records in regular listener equal to expected",
                        expRegCnt, regCnt.get());
                }

                assertTrue("Receive all expected events",
                    transUpdCnt.await(DFLT_LATCH_TIMEOUT, TimeUnit.MILLISECONDS));
                assertEquals("Count of updated records equal to expected", expTransCnt, transCnt.get());

            }
        }
        finally {
            stopAllGrids();
        }
    }

    private void populateData(IgniteCache<Integer, Employee> cache, String name) {
        for (int i = 0; i < DFLT_ENTRY_CNT; i++) {
            cache.put(i, new Employee(name + i, 42 * i));
        }
    }

    private static class RemoteTransformer implements IgniteClosure<Cache.Entry<Integer, Employee>, String> {
        @Override public String apply(Cache.Entry<Integer, Employee> entry) {
            return entry.getValue().name;
        }
    }

    private static class RemoteCacheEntryEventFilter implements CacheEntryEventSerializableFilter<Integer, Object> {
        @Override public boolean evaluate(
            CacheEntryEvent<? extends Integer, ?> event) throws CacheEntryListenerException {
            return event.getKey() % 2 == 0;
        }
    }

    private static class LocalTransformedEventListener implements TransformedEventListener<String> {
        private final AtomicInteger cnt;
        private final CountDownLatch cntLatch;

        LocalTransformedEventListener(AtomicInteger transCnt, CountDownLatch transUpdCnt) {
            this.cnt = transCnt;
            this.cntLatch = transUpdCnt;
        }

        @Override public void onUpdated(Iterable<? extends String> events) throws CacheEntryListenerException {
            for (String evt : events) {
                if (evt.contains(SARAH_CONNOR))
                    cnt.incrementAndGet();
                cntLatch.countDown();
            }
        }
    }

    private static class LocalCacheEntryUpdatedListener implements CacheEntryUpdatedListener<Object, Object> {
        private final AtomicInteger cnt;
        private final CountDownLatch countLatch;
        private final boolean keepBinary;

        LocalCacheEntryUpdatedListener(AtomicInteger regCnt, CountDownLatch regUpdCount, boolean keepBinary) {
            this.cnt = regCnt;
            this.countLatch = regUpdCount;
            this.keepBinary = keepBinary;
        }

        @Override public void onUpdated(
            Iterable<CacheEntryEvent<?, ?>> events) throws CacheEntryListenerException {
            for (CacheEntryEvent<?, ?> evt : events) {
                if (keepBinary)
                    binaryEvent((CacheEntryEvent<Integer, BinaryObject>)evt);
                else
                    typedEvent((CacheEntryEvent<Integer, Employee>)evt);
            }
        }

        private void binaryEvent(CacheEntryEvent<Integer, BinaryObject> evt) {
            checkName((String)evt.getValue().field("name"));
        }

        protected void typedEvent(CacheEntryEvent<Integer, Employee> evt) {
            checkName(evt.getValue().name);
        }

        private void checkName(String name) {
            if (name.contains(SARAH_CONNOR))
                cnt.incrementAndGet();
            countLatch.countDown();
        }
    }

    public class Employee {
        public String name;
        public Integer salary;

        Employee(String name, Integer salary) {
            this.name = name;
            this.salary = salary;
        }
    }
}
