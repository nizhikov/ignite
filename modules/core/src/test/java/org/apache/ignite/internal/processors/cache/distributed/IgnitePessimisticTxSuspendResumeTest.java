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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public class IgnitePessimisticTxSuspendResumeTest extends AbstractTransactionsInMultipleThreadsTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        jcache(DEFAULT_NODE_ID).removeAll();
    }

    /**
     * Test for suspension on pessimistic transaction.
     *
     * @throws Exception If failed.
     */
    public void testSuspendPessimisticTransaction() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyx(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, isolation)) {
                    cache.put(1, "1");

                    tx.suspend();

                    fail("Suspend must fail, because it isn't supported for pessimistic transactions.");
                }
                catch (Throwable e) {
                    if (!X.hasCause(e, UnsupportedOperationException.class))
                        throw e;
                }

                assertNull(cache.get(1));
            }
        });
    }

    /**
     * Test for resuming on pessimistic transaction.
     *
     * @throws Exception If failed.
     */
    public void testResumePessimisticTransaction() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyx(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, isolation)) {
                    cache.put(1, "1");

                    tx.suspend();

                    GridTestUtils.runAsync(new Runnable() {
                        @Override public void run() {
                            tx.resume();
                        }
                    }).get(FUT_TIMEOUT);

                    fail("Resume must fail, because it isn't supported for pessimistic transactions.");
                }
                catch (UnsupportedOperationException ignored) {
                    // No-op.
                }

                assertNull(cache.get(1));
            }
        });
    }
}
