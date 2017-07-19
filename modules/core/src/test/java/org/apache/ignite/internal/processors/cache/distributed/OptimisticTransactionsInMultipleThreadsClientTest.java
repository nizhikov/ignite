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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
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
public class OptimisticTransactionsInMultipleThreadsClientTest extends OptimisticTransactionsInMultipleThreadsTest {
    /** Number of concurrently running threads, which tries to perform transaction operations. */
    private int concurrentThreadsNum = 25;

    public static final int DEFAULT_BARRIER_TIMEOUT = 10_000;
    //public static final int DEFAULT_NODE_ID = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getTestIgniteInstanceName(1), getConfiguration().setClientMode(true));

        awaitPartitionMapExchange();

    }

    /**
     * Test start 1 transaction, resuming it in another thread. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    public void testResumeTxWhileStartingAnotherTx() throws Exception {
        for (final TransactionIsolation firstTxIsolation : TransactionIsolation.values())
            runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
                @Override public void applyX(TransactionIsolation isolation) throws Exception {
                    final IgniteCache<String, Integer> clientCache = jcache(DEFAULT_NODE_ID);
                    final IgniteCache<String, Integer> remoteCache = jcache(0);

                    final CyclicBarrier barrier = new CyclicBarrier(2);

                    final String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

                    final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                    final Transaction clientTx = txs.txStart(OPTIMISTIC,
                        firstTxIsolation);

                    clientCache.put(remotePrimaryKey, 1);

                    clientTx.suspend();

                    final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            assertNull(txs.tx());
                            assertEquals(TransactionState.SUSPENDED, clientTx.state());

                            clientTx.resume();

                            clientCache.put(remotePrimaryKey, 2);

                            barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                            barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                            clientTx.commit();

                            return true;
                        }
                    });

                    barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                    final Transaction clientTx2 = ignite(DEFAULT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                    clientCache.put(remotePrimaryKey, 3);

                    clientTx2.commit();

                    barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                    fut.get(5000);

                    assertEquals(2, jcache(0).get(remotePrimaryKey));

                    jcache(0).removeAll();
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
        for (final TransactionIsolation firstTxIsolation : TransactionIsolation.values())
            runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
                @Override public void applyX(TransactionIsolation isolation) throws Exception {
                    final IgniteCache<String, Integer> clientCache = jcache(DEFAULT_NODE_ID);
                    final IgniteCache<String, Integer> remoteCache = jcache(0);

                    String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

                    Ignite clientIgnite = ignite(DEFAULT_NODE_ID);

                    final Transaction clientTx = clientIgnite.transactions().txStart(OPTIMISTIC, firstTxIsolation);

                    clientCache.put(remotePrimaryKey, 1);

                    clientTx.suspend();

                    final Transaction clientTx2 = clientIgnite.transactions().txStart(OPTIMISTIC, isolation);

                    clientCache.put(remotePrimaryKey, 2);

                    clientTx2.commit();

                    assertEquals(2, jcache(0).get(remotePrimaryKey));

                    clientTx.resume();

                    clientTx.close();

                    remoteCache.removeAll();
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
                final IgniteCache<String, Integer> clientCache = jcache(DEFAULT_NODE_ID);
                final IgniteCache<String, Integer> remoteCache = jcache(0);

                final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum + 1);
                final LongAdder8 failedTxNumber = new LongAdder8();
                final AtomicInteger threadCnt = new AtomicInteger();
                final AtomicInteger successfulResume = new AtomicInteger();

                String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

                final Transaction clientTx = ignite(DEFAULT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(remotePrimaryKey, 1);

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failedTxNumber);

                        return null;
                    }
                }, concurrentThreadsNum, "th-suspendTx");

                barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                clientTx.suspend();

                fut.get();

                // if transaction was not closed after resume, then close it now.
                if (successfulResume.get() == 0) {
                    clientTx.resume();

                    clientTx.close();
                }

                assertTrue(successfulResume.get() < 2);
                assertEquals(concurrentThreadsNum, failedTxNumber.intValue() + successfulResume.intValue());
                assertNull(remoteCache.get(remotePrimaryKey));
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
                final IgniteCache<String, Integer> clientCache = jcache(DEFAULT_NODE_ID);
                final IgniteCache<String, Integer> remoteCache = jcache(0);

                final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum);
                final LongAdder8 failNumber = new LongAdder8();
                final AtomicInteger threadCnt = new AtomicInteger();
                final AtomicInteger successfulResume = new AtomicInteger();

                String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

                final Transaction clientTx = ignite(DEFAULT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(remotePrimaryKey, 1);

                clientTx.suspend();

                multithreaded(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failNumber);

                        return null;
                    }
                }, concurrentThreadsNum);

                assertEquals(1, successfulResume.get());
                assertEquals(concurrentThreadsNum - 1, failNumber.intValue());
                assertNull(remoteCache.get(remotePrimaryKey));
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
                final IgniteCache<String, Integer> clientCache = jcache(DEFAULT_NODE_ID);
                final IgniteCache<String, Integer> remoteCache = jcache(0);

                final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum + 1);
                final LongAdder8 failNumber = new LongAdder8();
                final AtomicInteger threadCnt = new AtomicInteger();
                final AtomicInteger successfulResume = new AtomicInteger();

                String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

                final Transaction clientTx = ignite(DEFAULT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(remotePrimaryKey, 1);

                clientTx.suspend();

                multithreaded(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        clientTx.resume();

                        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failNumber);

                                return null;
                            }
                        }, concurrentThreadsNum, "th-commit");

                        barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                        clientTx.commit();

                        fut.get();

                        return null;
                    }
                }, 1);

                assertEquals(0, successfulResume.get());
                assertEquals(concurrentThreadsNum, failNumber.intValue());
                assertEquals(1, jcache(0).get(remotePrimaryKey));
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
                final IgniteCache<String, Integer> clientCache = jcache(DEFAULT_NODE_ID);
                final IgniteCache<String, Integer> remoteCache = jcache(0);

                final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum + 1);
                final LongAdder8 failNumber = new LongAdder8();
                final AtomicInteger threadCnt = new AtomicInteger();
                final AtomicInteger successfulResume = new AtomicInteger();

                String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

                final Transaction clientTx = ignite(DEFAULT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(remotePrimaryKey, 1);

                clientTx.suspend();

                multithreaded(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        clientTx.resume();

                        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failNumber);

                                return null;
                            }
                        }, concurrentThreadsNum, "th-rollback");

                        barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                        clientTx.rollback();

                        fut.get();

                        return null;
                    }
                }, 1);

                assertEquals(0, successfulResume.get());
                assertEquals(concurrentThreadsNum, failNumber.intValue());
                assertNull(jcache(0).get(remotePrimaryKey));
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
                final IgniteCache<String, Integer> clientCache = jcache(DEFAULT_NODE_ID);
                final IgniteCache<String, Integer> remoteCache = jcache(0);

                final CyclicBarrier barrier = new CyclicBarrier(concurrentThreadsNum + 1);
                final LongAdder8 failNumber = new LongAdder8();
                final AtomicInteger threadCnt = new AtomicInteger();
                final AtomicInteger successfulResume = new AtomicInteger();

                String remotePrimaryKey = String.valueOf(primaryKey(remoteCache));

                final Transaction clientTx = ignite(DEFAULT_NODE_ID).transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(remotePrimaryKey, 1);

                clientTx.suspend();

                multithreaded(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        clientTx.resume();

                        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                waitAndPerformOperation(threadCnt, barrier, clientTx, successfulResume, failNumber);

                                return null;
                            }
                        }, concurrentThreadsNum, "th-close");

                        barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                        clientTx.close();

                        fut.get();

                        return null;
                    }
                }, 1);

                assertEquals(0, successfulResume.get());
                assertEquals(concurrentThreadsNum, failNumber.intValue());
                assertNull(jcache(0).get(remotePrimaryKey));
            }
        });
    }

    /**
     * Thread begin waiting on barrier and then performs some operation.
     *
     * @param threadCnt Common counter for threads.
     * @param barrier Barrier, all threads are waiting on.
     * @param clientTx Transaction instance that we test.
     * @param successfulResume Counter for successful resume operations.
     * @param failedTxNumber Counter for failed operations.
     * @throws Exception If failed.
     */
    private void waitAndPerformOperation(AtomicInteger threadCnt, CyclicBarrier barrier, Transaction clientTx,
        AtomicInteger successfulResume, LongAdder8 failedTxNumber) throws Exception {
        try {
            int threadNum = threadCnt.incrementAndGet();

            switch (threadNum % 5) {
                case 0:
                    barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                    clientTx.suspend();

                    break;

                case 1:
                    barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                    clientTx.resume();

                    successfulResume.incrementAndGet();

                    clientTx.close();

                    return;

                case 2:
                    barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                    clientTx.commit();

                    break;

                case 3:
                    barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

                    clientTx.rollback();

                    break;

                case 4:
                    barrier.await(DEFAULT_BARRIER_TIMEOUT, MILLISECONDS);

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
