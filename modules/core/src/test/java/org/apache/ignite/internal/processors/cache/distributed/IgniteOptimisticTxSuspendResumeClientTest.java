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

import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 *
 */
public class IgniteOptimisticTxSuspendResumeClientTest extends IgniteOptimisticTxSuspendResumeTest {
    /** Cyclic barrier timeout */
    public static final int DEFAULT_BARRIER_TIMEOUT = 10_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getTestIgniteInstanceName(CLIENT_NODE_ID), getConfiguration().setClientMode(true));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        jcache(DEFAULT_NODE_ID).removeAll();
        jcache(CLIENT_NODE_ID).removeAll();
    }

    /**
     * Test start 1 transaction, resuming it in another thread. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxWhileStartingAnotherTx() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyx(final TransactionIsolation tx1Isolation) throws Exception {
                runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
                    @Override public void applyx(TransactionIsolation tx2Isolation) throws Exception {
                        final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                        final IgniteCache<Integer, String> remoteCache = jcache(DEFAULT_NODE_ID);

                        final CyclicBarrier barrier = new CyclicBarrier(2);

                        final IgniteTransactions txs = ignite(CLIENT_NODE_ID).transactions();

                        final Transaction clientTx = txs.txStart(OPTIMISTIC, tx1Isolation);

                        clientCache.put(1, "1");

                        clientTx.suspend();

                        final IgniteInternalFuture fut = GridTestUtils.runAsync(new RunnableX() {
                            @Override public void runx() throws Exception {
                                assertNull(txs.tx());
                                assertEquals(TransactionState.SUSPENDED, clientTx.state());

                                clientTx.resume();

                                clientCache.put(1, "2");

                                barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                                barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                                clientTx.commit();
                            }
                        });

                        barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                        final Transaction clientTx2 = txs.txStart(OPTIMISTIC, tx2Isolation);

                        clientCache.put(1, "3");

                        clientTx2.commit();

                        barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                        fut.get(FUT_TIMEOUT);

                        assertEquals("2", remoteCache.get(1));

                        remoteCache.removeAll();
                    }
                });
            }
        });
    }

    /**
     * Test start 1 transaction, suspendTx it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testSuspendTxAndStartNewTx() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyx(final TransactionIsolation tx1Isolation) throws Exception {
                runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
                    @Override public void applyx(TransactionIsolation tx2Isolation) throws Exception {
                        final Ignite remoteIgnite = ignite(DEFAULT_NODE_ID);
                        final IgniteCache<Integer, String> remoteCache = remoteIgnite.cache(DEFAULT_CACHE_NAME);

                        final Ignite clientIgnite = ignite(CLIENT_NODE_ID);
                        final IgniteCache<Integer, String> clientCache = clientIgnite.cache(DEFAULT_CACHE_NAME);

                        final Transaction clientTx = clientIgnite.transactions().txStart(OPTIMISTIC, tx1Isolation);

                        clientCache.put(1, "1");

                        clientTx.suspend();

                        final Transaction clientTx2 = clientIgnite.transactions().txStart(OPTIMISTIC, tx2Isolation);

                        clientCache.put(1, "2");

                        clientTx2.commit();

                        assertEquals("2", remoteCache.get(1));

                        clientTx.resume();

                        clientTx.close();

                        remoteCache.removeAll();
                    }
                });
            }
        });
    }

    /**
     * Test for concurrent transaction suspendTx.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentSuspend() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyx(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final Transaction clientTx = ignite(CLIENT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                final boolean[] opSuc = {false};
                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyx(Integer idx) throws Exception {
                        try {
                            suspendedTxProhibitedOps.get(idx).apply(clientTx);

                            opSuc[0] = true;
                        }
                        catch (Exception ignored) {
                            // No-op.
                        }
                        catch (AssertionError ignored) {
                            // No-op.
                        }
                    }
                }, suspendedTxProhibitedOps.size(), "th-suspendTx");

                assertFalse(opSuc[0]);

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyx(Integer idx) throws Exception {
                        clientTx.resume();
                        clientTx.close();
                    }
                }, 1, "th-suspendTx");

                assertNull(clientCache.get(1));
            }
        });
    }

    /**
     * Test for concurrent transaction commit.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentCommit() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyx(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final IgniteCache<Integer, String> remoteCache = jcache(DEFAULT_NODE_ID);

                final Transaction clientTx = ignite(DEFAULT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyx(Integer threadNum) throws Exception {
                        clientTx.resume();
                        clientTx.commit();

                        final boolean[] opSuc = {false};
                        GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                            @Override public void applyx(Integer idx) throws Exception {
                                try {
                                    suspendedTxProhibitedOps.get(idx).apply(clientTx);

                                    opSuc[0] = true;
                                }
                                catch (Exception ignored) {
                                    // No-op.
                                }
                                catch (AssertionError ignored) {
                                    // No-op.
                                }
                            }
                        }, suspendedTxProhibitedOps.size(), "th-commit");

                        assertFalse(opSuc[0]);
                    }
                }, 1, "th-commit-outer");

                assertEquals("1", remoteCache.get(1));
            }
        });
    }

    /**
     * Test for concurrent transaction rollback.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentRollback() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyx(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final Transaction clientTx = ignite(CLIENT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyx(Integer threadNum) throws Exception {
                        clientTx.resume();
                        clientTx.rollback();

                        final boolean[] opSuc = {false};
                        GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                            @Override public void applyx(Integer idx) throws Exception {
                                try {
                                    suspendedTxProhibitedOps.get(idx).apply(clientTx);

                                    opSuc[0] = true;
                                }
                                catch (Exception ignored) {
                                    // No-op.
                                }
                                catch (AssertionError ignored) {
                                    // No-op.
                                }
                            }
                        }, suspendedTxProhibitedOps.size(), "th-rollback");

                        assertFalse(opSuc[0]);
                    }
                }, 1, "th-rollback-outer");

                assertNull(clientCache.get(1));
            }
        });
    }

    /**
     * Test for concurrent transaction close.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentClose() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyx(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final Transaction clientTx = ignite(CLIENT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyx(Integer threadNum) throws Exception {
                        clientTx.resume();
                        clientTx.close();

                        final boolean[] opSuc = {false};
                        GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                            @Override public void applyx(Integer idx) throws Exception {
                                try {
                                    suspendedTxProhibitedOps.get(idx).apply(clientTx);

                                    opSuc[0] = true;
                                }
                                catch (Exception ignored) {
                                    // No-op.
                                }
                                catch (AssertionError ignored) {
                                    // No-op.
                                }
                            }
                        }, suspendedTxProhibitedOps.size(), "th-close");
                    }
                }, 1, "th-close-outer");

                assertNull(clientCache.get(1));
            }
        });
    }
}
