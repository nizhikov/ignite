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

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jsr166.LongAdder8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 *
 */
public class IgniteOptimisticTxSuspendResumeClientTest extends IgniteOptimisticTxSuspendResumeTest {
    /** Number of concurrently running threads, which tries to perform transaction operations. */
    private int concurrentThreadsNum = 25;

    public static final int DEFAULT_BARRIER_TIMEOUT = 10_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getTestIgniteInstanceName(CLIENT_NODE_ID), getConfiguration().setClientMode(true));

        awaitPartitionMapExchange();
    }

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
            @Override public void applyX(final TransactionIsolation isolation1) throws Exception {
                runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
                    @Override public void applyX(TransactionIsolation isolation2) throws Exception {
                        final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                        final IgniteCache<Integer, String> remoteCache = jcache(DEFAULT_NODE_ID);

                        final CyclicBarrier barrier = new CyclicBarrier(2);

                        final IgniteTransactions txs = ignite(CLIENT_NODE_ID).transactions();

                        final Transaction clientTx = txs.txStart(OPTIMISTIC, isolation1);

                        clientCache.put(1, "1");

                        clientTx.suspend();

                        final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                            @Override public Boolean call() throws Exception {
                                assertNull(txs.tx());
                                assertEquals(TransactionState.SUSPENDED, clientTx.state());

                                clientTx.resume();

                                clientCache.put(1, "2");

                                barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                                barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                                clientTx.commit();

                                return true;
                            }
                        });

                        barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                        final Transaction clientTx2 = txs.txStart(OPTIMISTIC, isolation2);

                        clientCache.put(1, "3");

                        clientTx2.commit();

                        barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                        assertTrue(fut.get(5000));
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
            @Override public void applyX(final TransactionIsolation isolation1) throws Exception {
                runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
                    @Override public void applyX(TransactionIsolation isolation2) throws Exception {
                        final Ignite remoteIgnite = ignite(DEFAULT_NODE_ID);
                        final IgniteCache<Integer, String> remoteCache = remoteIgnite.cache(DEFAULT_CACHE_NAME);

                        final Ignite clientIgnite = ignite(CLIENT_NODE_ID);
                        final IgniteCache<Integer, String> clientCache = clientIgnite.cache(DEFAULT_CACHE_NAME);

                        final Transaction clientTx = clientIgnite.transactions().txStart(OPTIMISTIC, isolation1);

                        clientCache.put(1, "1");

                        clientTx.suspend();

                        final Transaction clientTx2 = clientIgnite.transactions().txStart(OPTIMISTIC, isolation2);

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
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final LongAdder8 failedTxNumber = new LongAdder8();

                final AtomicInteger successfulResume = new AtomicInteger();

                final Transaction clientTx = ignite(CLIENT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyX(Integer threadNum) throws Exception {
                        waitAndPerformOperation(threadNum, clientTx, successfulResume, failedTxNumber);
                    }
                }, concurrentThreadsNum, "th-suspendTx");

                // if transaction was not closed after resume, then close it now.
                if (successfulResume.get() == 0) {
                    clientTx.resume();

                    clientTx.close();
                }

                assertEquals(1, successfulResume.get());
                assertEquals(concurrentThreadsNum, failedTxNumber.intValue() + successfulResume.intValue());
                assertNull(clientCache.get(1));
            }
        });
    }

    /**
     * Test for concurrent transaction resume.
     *
     * @throws Exception If failed.
     */
    public void testTxConcurrentResume() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final LongAdder8 failNumber = new LongAdder8();

                final AtomicInteger successfulResume = new AtomicInteger();

                final Transaction clientTx = ignite(CLIENT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyX(Integer threadNum) throws Exception {
                        waitAndPerformOperation(threadNum, clientTx, successfulResume, failNumber);
                    }
                }, concurrentThreadsNum, "th-resumeTx");

                assertEquals(1, successfulResume.get());
                assertEquals(concurrentThreadsNum - 1, failNumber.intValue());
                assertFalse(clientCache.containsKey(1));
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
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final IgniteCache<Integer, String> remoteCache = jcache(DEFAULT_NODE_ID);

                final LongAdder8 failNumber = new LongAdder8();

                final AtomicInteger successfulResume = new AtomicInteger();

                final Transaction clientTx = ignite(DEFAULT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyX(Integer threadNum) throws Exception {
                        clientTx.resume();
                        clientTx.commit();

                        GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                            @Override public void applyX(Integer threadNum) throws Exception {
                                waitAndPerformOperation(threadNum, clientTx, successfulResume, failNumber);
                            }
                        }, concurrentThreadsNum, "th-commit");
                    }
                }, 1, "th-commit-outer");

                assertEquals(0, successfulResume.get());
                assertEquals(concurrentThreadsNum, failNumber.intValue());
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
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final LongAdder8 failNumber = new LongAdder8();

                final AtomicInteger successfulResume = new AtomicInteger();

                final Transaction clientTx = ignite(CLIENT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyX(Integer threadNum) throws Exception {
                        clientTx.resume();
                        clientTx.rollback();

                        GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                            @Override public void applyX(Integer threadNum) throws Exception {
                                waitAndPerformOperation(threadNum, clientTx, successfulResume, failNumber);
                            }
                        }, concurrentThreadsNum, "th-rollback");

                    }
                }, 1, "th-rollback-outer");


                assertEquals(0, successfulResume.get());
                assertEquals(concurrentThreadsNum, failNumber.intValue());
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
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> clientCache = jcache(CLIENT_NODE_ID);

                final LongAdder8 failNumber = new LongAdder8();

                final AtomicInteger successfulResume = new AtomicInteger();

                final Transaction clientTx = ignite(CLIENT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1, "1");

                clientTx.suspend();

                GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                    @Override public void applyX(Integer threadNum) throws Exception {
                        clientTx.resume();
                        clientTx.close();

                        GridTestUtils.runMultiThreaded(new CI1Exc<Integer>() {
                            @Override public void applyX(Integer threadNum) throws Exception {
                                waitAndPerformOperation(threadNum, clientTx, successfulResume, failNumber);
                            }
                        }, concurrentThreadsNum, "th-close");

                    }
                }, 1, "th-close-outer");

                assertEquals(0, successfulResume.get());
                assertEquals(concurrentThreadsNum, failNumber.intValue());
                assertNull(clientCache.get(1));
            }
        });
    }

    /**
     * Thread begin waiting on barrier and then performs some operation.
     *
     * @param clientTx Transaction instance that we test.
     * @param successfulResume Counter for successful resume operations.
     * @param failedTxNumber Counter for failed operations.
     * @throws Exception If failed.
     */
    private void waitAndPerformOperation(int threadNum, Transaction clientTx,
        AtomicInteger successfulResume, LongAdder8 failedTxNumber) throws Exception {
        try {
            switch (threadNum % 5) {
                case 0:
                    clientTx.suspend();

                    break;

                case 1:
                    clientTx.resume();

                    successfulResume.incrementAndGet();

                    clientTx.close();

                    return;

                case 2:
                    clientTx.commit();

                    break;

                case 3:
                    clientTx.rollback();

                    break;

                case 4:
                    clientTx.close();

                    break;

                default:
                    assert false;
            }

            fail("Concurrent operation must failed, because it doesn't own transaction.");
        }
        catch (Throwable e) {
            failedTxNumber.increment();
        }
    }
}
