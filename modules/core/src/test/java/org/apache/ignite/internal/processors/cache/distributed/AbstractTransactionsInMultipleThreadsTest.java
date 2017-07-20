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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public abstract class AbstractTransactionsInMultipleThreadsTest extends GridCommonAbstractTest {
    /**
     * Default node id.
     */
    public static final int DEFAULT_NODE_ID = 0;

    /**
     * Client node id.
     */
    public static final int CLIENT_NODE_ID = 1;

    /** Transaction timeout. */
    public static final long TX_TIMEOUT = 100;

    /** Future timeout */
    public static final int FUT_TIMEOUT = 5000;

    /**
     * List of closures that execute some transaction operation
     */
    protected List<CI1Exc<Transaction>> suspendedTxProhibitedOps = Arrays.asList(
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.suspend();
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.close();
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.commit();
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.commitAsync().get(FUT_TIMEOUT);
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.rollback();
            }
        },
        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.rollbackAsync().get(FUT_TIMEOUT);
            }
        },

        new CI1Exc<Transaction>() {
            @Override public void applyx(Transaction tx) throws Exception {
                tx.setRollbackOnly();
            }
        }
    );

    /**
     * Creates new cache configuration.
     *
     * @return CacheConfiguration New cache configuration.
     */
    protected CacheConfiguration<Integer, String> getCacheConfiguration() {
        CacheConfiguration<Integer, String> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(false);
        cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //TODO: remove to test
        checkAllTransactionsHasEnded();
    }

    /**
     * Checks whether all transactions has ended.
     */
    private void checkAllTransactionsHasEnded() {
        for (Ignite ignite : G.allGrids()) {
            GridCacheSharedContext<Object, Object> cctx = ((IgniteKernal)ignite).context().cache().context();

            IgniteTxManager txMgr = cctx.tm();

            assertTrue(txMgr.activeTransactions().isEmpty());
        }
    }

    /**
     * Starts test scenario for all transaction isolation levels.
     *
     * @param testScenario Test scenario.
     * @throws Exception If scenario failed.
     */
    protected void runWithAllIsolations(IgniteInClosure<TransactionIsolation> testScenario) throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values())
            testScenario.apply(isolation);
    }

    /**
     * Closure that can throw any exception
     *
     * @param <T> type of closure parameter
     */
    public abstract class CI1Exc<T> implements CI1<T> {
        public abstract void applyx(T o) throws Exception;

        @Override public void apply(T o) {
            try {
                applyx(o);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Runnable that can throw any exception
     */
    public abstract class RunnableX implements Runnable {
        abstract void runx() throws Exception;

        @Override public void run() {
            try {
                runx();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
