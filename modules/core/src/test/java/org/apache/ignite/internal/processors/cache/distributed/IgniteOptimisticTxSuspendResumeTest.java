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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/**
 *
 */
public class IgniteOptimisticTxSuspendResumeTest extends AbstractTransactionsInMultipleThreadsTest {
    /** Name for test cache */
    private static final String TEST_CACHE_NAME = "testCache";

    /** Name for second test cache */
    private static final String TEST_CACHE_NAME2 = "testCache2";

    /** Transaction timeout. */
    private static final long TIMEOUT = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids(true);
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        jcache(DEFAULT_NODE_ID).removeAll();
    }

    /**
     * Test for transaction starting in one thread, continuing in another.
     *
     * @throws Exception If failed.
     */
    public void testSimpleTransactionInAnotherThread() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                cache.put(1, "1");
                cache.put(2, "2");

                tx.suspend();

                assertNull(cache.get(1));

                IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        assertNull(txs.tx());
                        assertEquals(SUSPENDED, tx.state());

                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        cache.put(3, "3");
                        cache.remove(2);

                        tx.commit();

                        return true;
                    }
                });

                fut.get(5000);

                assertEquals(COMMITTED, tx.state());
                assertEquals("1", cache.get(1));
                assertEquals("3", cache.get(3));
                assertFalse(cache.containsKey(2));

                cache.removeAll();
            }
        });
    }

    /**
     * Test for transaction starting in one thread, continuing in another, and resuming in initiating thread.
     *
     * @throws Exception If failed.
     */
    public void testSimpleTransactionInAnotherThreadContinued() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                cache.put(1, "1");
                cache.put(4, "4");

                tx.suspend();

                assertNull(cache.get(1));

                IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        assertNull(txs.tx());
                        assertEquals(SUSPENDED, tx.state());

                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        cache.put(2, "2");
                        cache.remove(4, "4");
                        cache.put(5, "5");
                        cache.put(6, "6");

                        tx.suspend();

                        return true;
                    }
                });

                fut.get(5000);

                assertNull(txs.tx());
                assertEquals(SUSPENDED, tx.state());

                tx.resume();

                assertEquals(ACTIVE, tx.state());

                cache.put(3, "3");
                cache.remove(5, "5");
                cache.remove(6, "6");

                tx.commit();

                assertEquals(COMMITTED, tx.state());
                assertEquals("1", cache.get(1));
                assertEquals("3", cache.get(3));
                assertEquals("3", cache.get(3));
                assertFalse(cache.containsKey(4));
                assertFalse(cache.containsKey(5));
                assertFalse(cache.containsKey(6));

                cache.removeAll();
            }
        });
    }

    /**
     * Test for transaction starting in one thread, continuing in another. Cache operations performed for a couple of
     * caches.
     *
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThread() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final Ignite ignite = ignite(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite.transactions();

                final IgniteCache<Integer, String> cache1 = ignite.getOrCreateCache(getCacheConfiguration().setName(TEST_CACHE_NAME));

                final IgniteCache<Integer, String> cache2 = ignite.getOrCreateCache(getCacheConfiguration().setName(TEST_CACHE_NAME2));

                final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                cache1.put(1, "1");
                cache2.put(2, "2");

                tx.suspend();

                IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        assertNull(txs.tx());
                        assertEquals(SUSPENDED, tx.state());

                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        cache1.put(3, "3");
                        cache2.remove(2);

                        tx.commit();

                        return true;
                    }
                });

                fut.get(5000);

                assertEquals(COMMITTED, tx.state());
                assertEquals("1", cache1.get(1));
                assertEquals("3", cache1.get(3));
                assertFalse(cache2.containsKey(2));

                cache2.removeAll();
                cache1.removeAll();
            }
        });
    }

    /**
     * Test for transaction starting in one thread, continuing in another, and resuming in initiating thread.
     * Cache operations performed for a couple of caches.
     *
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThreadContinued() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final Ignite ignite = ignite(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite.transactions();

                final IgniteCache<Integer, String> cache1 = ignite.getOrCreateCache(getCacheConfiguration().setName(TEST_CACHE_NAME));

                final IgniteCache<Integer, String> cache2 = ignite.getOrCreateCache(getCacheConfiguration().setName(TEST_CACHE_NAME2));

                final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                cache1.put(1, "1");
                cache1.put(2, "2");

                cache2.put(11, "11");

                tx.suspend();

                IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        assertNull(txs.tx());
                        assertEquals(SUSPENDED, tx.state());

                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        cache1.put(3, "3");
                        cache1.put(4, "4");

                        cache2.put(12, "12");
                        cache2.remove(11);

                        tx.suspend();

                        return true;
                    }
                });

                fut.get(5000);

                assertNull(txs.tx());
                assertEquals(SUSPENDED, tx.state());

                tx.resume();

                assertEquals(ACTIVE, tx.state());

                cache1.remove(4, "4");

                cache2.remove(12, "12");

                tx.commit();

                assertEquals(COMMITTED, tx.state());
                assertEquals("1", cache1.get(1));
                assertEquals("2", cache1.get(2));
                assertEquals("3", cache1.get(3));
                assertFalse(cache1.containsKey(4));
                assertFalse(cache2.containsKey(11));
                assertFalse(cache2.containsKey(12));

                cache1.removeAll();

                cache2.removeAll();
            }
        });
    }

    /**
     * Test for transaction rollback.
     *
     * @throws Exception If failed.
     */
    public void testTransactionRollback() throws Exception {
        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                cache.put(1, "1");
                cache.put(2, "2");

                tx.suspend();

                final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        assertNull(txs.tx());
                        assertEquals(SUSPENDED, tx.state());

                        tx.resume();

                        assertEquals(ACTIVE, tx.state());

                        cache.put(3, "3");

                        assertTrue(cache.remove(2));

                        tx.rollback();

                        return true;
                    }
                });

                fut.get(5000);

                assertEquals(ROLLED_BACK, tx.state());
                assertFalse(cache.containsKey(1));
                assertFalse(cache.containsKey(2));
                assertFalse(cache.containsKey(3));

                cache.removeAll();
            }
        });
    }

    /**
     * Test for starting and suspending transactions, and then resuming and committing in another thread.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testMultipleTransactionsSuspendResume() throws IgniteCheckedException {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final List<Transaction> txs = new ArrayList<>();

            IgniteCache<Integer, String> clientCache = jcache(DEFAULT_NODE_ID);

            Ignite clientNode = ignite(DEFAULT_NODE_ID);

            Transaction clientTx;

            for (int i = 0; i < 10; i++) {
                clientTx = clientNode.transactions().txStart(OPTIMISTIC, isolation);

                clientCache.put(1,  String.valueOf(i));

                clientTx.suspend();

                txs.add(clientTx);
            }

            final IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    assertNull(ignite(DEFAULT_NODE_ID).transactions().tx());

                    for (int i = 0; i < 10; i++) {
                        Transaction clientTx = txs.get(i);

                        assertEquals(SUSPENDED, clientTx.state());

                        clientTx.resume();

                        assertEquals(ACTIVE, clientTx.state());

                        clientTx.commit();
                    }

                    return true;
                }
            });

            fut.get(5000);

            assertEquals("9", jcache(DEFAULT_NODE_ID).get(1));

            clientCache.removeAll();
        }
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction from the other thread are prohibited.
     */
    public void testOperationsAreProhibitedOnSuspendedTxFromTheOtherThread() throws Exception {
        for (int opIdx = 0; opIdx < 7; opIdx++)
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                try {
                    cache.put(1, "1");

                    tx.suspend();

                    final int finalOpIdx = opIdx;
                    multithreaded(new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            performOperation(tx, finalOpIdx);

                            return null;
                        }
                    }, 1);

                    fail("Operation on suspended transaction is prohibited from the other thread.");
                }
                catch (Throwable ignore) {
                    // ignoring exception on suspended transaction.
                }

                tx.resume();

                tx.close();

                assertNull(cache.get(1));
            }
    }

    /**
     * Test checking timeout on resumed transaction.
     *
     * @throws Exception If failed.
     */
    public void testTransactionTimeoutOnResumedTransaction() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

            boolean tryResume = false;

            try (Transaction tx = txs.txStart(OPTIMISTIC, isolation, TIMEOUT, 0)) {
                tx.suspend();

                long sleep = TIMEOUT * 2;

                Thread.sleep(sleep);

                tryResume = true;

                tx.resume();

                fail("Transaction must have timed out.");
            }
            catch (Exception e) {
                if (!(X.hasCause(e, TransactionTimeoutException.class)))
                    throw e;
            }

            assertTrue(tryResume);
        }
    }

    /**
     * Test checking timeout on suspended transaction.
     *
     * @throws Exception If failed.
     */
    public void testTransactionTimeoutOnSuspendedTransaction() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

            final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

            boolean trySuspend = false;

            try (Transaction tx = txs.txStart(OPTIMISTIC, isolation, TIMEOUT, 0)) {
                cache.put(1, "1");

                long sleep = TIMEOUT * 2;

                Thread.sleep(sleep);

                trySuspend = true;

                tx.suspend();

                fail("Transaction must have timed out.");
            }
            catch (Exception e) {
                if (!(X.hasCause(e, TransactionTimeoutException.class)))
                    throw e;
            }

            assertNull(cache.get(1));

            assert trySuspend;
        }
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction are prohibited.
     */
    public void testOperationsAreProhibitedOnSuspendedTx() {
        for (int opIdx = 0; opIdx < 7; opIdx++)
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                try {
                    cache.put(1, "1");

                    tx.suspend();

                    performOperation(tx, opIdx);

                    fail("Operation on suspended transaction is prohibited.");
                }
                catch (Throwable ignore) {
                    // ignoring exception on suspended transaction.
                }

                tx.resume();

                tx.close();

                assertNull(cache.get(1));
            }
    }

    /**
     * Performs operation based on its index. Resume operation is not supported.
     *
     * @param tx Transaction, operation is performed on.
     * @param opIdx Operation index.
     */
    private void performOperation(Transaction tx, final int opIdx) {
        switch (opIdx) {
            case 0:
                tx.suspend();

                break;

            case 1:
                tx.close();

                break;

            case 2:
                tx.commit();

                break;

            case 3:
                tx.commitAsync();

                break;

            case 4:

                tx.rollback();

                break;

            case 5:
                tx.rollbackAsync();

                break;
            case 6:
                tx.setRollbackOnly();

                break;

            default:
                assert false;
        }
    }
}
