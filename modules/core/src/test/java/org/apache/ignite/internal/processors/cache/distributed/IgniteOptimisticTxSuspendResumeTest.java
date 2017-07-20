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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
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
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(DEFAULT_NODE_ID);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids(true);
    }

    /** {@inheritDoc} */
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
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

            final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

            final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

            cache.put(1, "1");
            cache.put(2, "2");

            tx.suspend();

            assertNull(cache.get(1));

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    assertNull(txs.tx());

                    assertEquals(SUSPENDED, tx.state());

                    tx.resume();

                    assertEquals(ACTIVE, tx.state());

                    cache.put(3, "3");

                    cache.remove(2);

                    tx.commit();
                }
            }).get(FUT_TIMEOUT);

            assertEquals(COMMITTED, tx.state());

            assertEquals("1", cache.get(1));
            assertEquals("3", cache.get(3));

            assertFalse(cache.containsKey(2));

            cache.removeAll();
        }
    }

    /**
     * Test for transaction starting in one thread, continuing in another, and resuming in initiating thread.
     *
     * @throws Exception If failed.
     */
    public void testSimpleTransactionInAnotherThreadContinued() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

            final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

            final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

            cache.put(1, "1");
            cache.put(4, "4");

            tx.suspend();

            assertNull(cache.get(1));

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    assertNull(txs.tx());

                    assertEquals(SUSPENDED, tx.state());

                    tx.resume();

                    assertEquals(ACTIVE, tx.state());

                    cache.put(2, "2");
                    cache.remove(4, "4");
                    cache.put(5, "5");
                    cache.put(6, "6");

                    tx.suspend();
                }
            }).get(FUT_TIMEOUT);

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
    }

    /**
     * Test for transaction starting in one thread, continuing in another. Cache operations performed for a couple of
     * caches.
     *
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThread() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final Ignite ignite = ignite(DEFAULT_NODE_ID);

            final IgniteTransactions txs = ignite.transactions();

            final IgniteCache<Integer, String> cache1 =
                ignite.getOrCreateCache(getCacheConfiguration().setName("cache1"));

            final IgniteCache<Integer, String> cache2 =
                ignite.getOrCreateCache(getCacheConfiguration().setName("cache2"));

            final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

            cache1.put(1, "1");
            cache2.put(2, "2");

            tx.suspend();

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    assertNull(txs.tx());

                    assertEquals(SUSPENDED, tx.state());

                    tx.resume();

                    assertEquals(ACTIVE, tx.state());

                    cache1.put(3, "3");

                    cache2.remove(2);

                    tx.commit();
                }
            }).get(FUT_TIMEOUT);

            assertEquals(COMMITTED, tx.state());

            assertEquals("1", cache1.get(1));
            assertEquals("3", cache1.get(3));

            assertFalse(cache2.containsKey(2));

            cache2.removeAll();

            cache1.removeAll();
        }
    }

    /**
     * Test for transaction starting in one thread, continuing in another, and resuming in initiating thread.
     * Cache operations performed for a couple of caches.
     *
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThreadContinued() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final Ignite ignite = ignite(DEFAULT_NODE_ID);

            final IgniteTransactions txs = ignite.transactions();

            final IgniteCache<Integer, String> cache1 =
                ignite.getOrCreateCache(getCacheConfiguration().setName("cache1"));

            final IgniteCache<Integer, String> cache2 =
                ignite.getOrCreateCache(getCacheConfiguration().setName("cache2"));

            final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

            cache1.put(1, "1");
            cache1.put(2, "2");

            cache2.put(11, "11");

            tx.suspend();

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    assertNull(txs.tx());

                    assertEquals(SUSPENDED, tx.state());

                    tx.resume();

                    assertEquals(ACTIVE, tx.state());

                    cache1.put(3, "3");
                    cache1.put(4, "4");

                    cache2.put(12, "12");
                    cache2.remove(11);

                    tx.suspend();
                }
            }).get(FUT_TIMEOUT);

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
    }

    /**
     * Test for transaction rollback.
     *
     * @throws Exception If failed.
     */
    public void testTransactionRollback() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

            final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

            final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

            cache.put(1, "1");
            cache.put(2, "2");

            tx.suspend();

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    assertNull(txs.tx());

                    assertEquals(SUSPENDED, tx.state());

                    tx.resume();

                    assertEquals(ACTIVE, tx.state());

                    cache.put(3, "3");

                    assertTrue(cache.remove(2));

                    tx.rollback();
                }
            }).get(FUT_TIMEOUT);

            assertEquals(ROLLED_BACK, tx.state());

            assertFalse(cache.containsKey(1));
            assertFalse(cache.containsKey(2));
            assertFalse(cache.containsKey(3));

            cache.removeAll();
        }
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

                clientCache.put(1, String.valueOf(i));

                clientTx.suspend();

                txs.add(clientTx);
            }

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    assertNull(ignite(DEFAULT_NODE_ID).transactions().tx());

                    for (int i = 0; i < 10; i++) {
                        Transaction clientTx = txs.get(i);

                        assertEquals(SUSPENDED, clientTx.state());

                        clientTx.resume();

                        assertEquals(ACTIVE, clientTx.state());

                        clientTx.commit();
                    }
                }
            }).get(FUT_TIMEOUT);

            assertEquals("9", jcache(DEFAULT_NODE_ID).get(1));

            clientCache.removeAll();
        }
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction from the other thread are prohibited.
     *
     * @throws Exception If failed.
     */
    public void testOperationsAreProhibitedOnSuspendedTxFromTheOtherThread() throws Exception {
        for (final CI1Exc<Transaction> txOperation : suspendedTxProhibitedOps) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                cache.put(1, "1");

                tx.suspend();

                final boolean[] opSuc = {false};
                multithreaded(new RunnableX() {
                    @Override public void runx() throws Exception {
                        try {
                            txOperation.apply(tx);

                            opSuc[0] = true;
                        }
                        catch (Exception ignored) {
                            // No-op.
                        }
                        catch (AssertionError ignored) {
                            // No-op.
                        }
                    }
                }, 1);

                assertFalse(opSuc[0]);

                tx.resume();
                tx.close();

                assertNull(cache.get(1));
            }
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

            try (Transaction tx = txs.txStart(OPTIMISTIC, isolation, TX_TIMEOUT, 0)) {
                tx.suspend();

                long sleep = TX_TIMEOUT * 2;

                Thread.sleep(sleep);

                tx.resume();

                fail("Transaction must have timed out.");
            }
            catch (TransactionTimeoutException ignored) {
                // No-op.
            }
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

            try (Transaction tx = txs.txStart(OPTIMISTIC, isolation, TX_TIMEOUT, 0)) {
                cache.put(1, "1");

                long sleep = TX_TIMEOUT * 2;

                Thread.sleep(sleep);

                tx.suspend();

                fail("Transaction must have timed out.");
            }
            catch (TransactionTimeoutException ignored) {
                // No-op.
            }

            assertNull(cache.get(1));
        }
    }

    /**
     * Test checking all operations(exception resume) on suspended transaction are prohibited.
     *
     * @throws Exception If failed.
     */
    public void testOperationsAreProhibitedOnSuspendedTx() throws Exception {
        for (CI1Exc<Transaction> txOperation : suspendedTxProhibitedOps) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                final IgniteCache<Integer, String> cache = jcache(DEFAULT_NODE_ID);

                final IgniteTransactions txs = ignite(DEFAULT_NODE_ID).transactions();

                Transaction tx = txs.txStart(OPTIMISTIC, isolation);

                cache.put(1, "1");

                tx.suspend();

                boolean opSuc = false;
                try {
                    txOperation.apply(tx);

                    opSuc = true;
                }
                catch (Exception ignored) {
                    // No-op.
                }
                catch (AssertionError ignored) {
                    // No-op.
                }

                assertFalse(opSuc);

                tx.resume();

                tx.close();

                assertNull(cache.get(1));
            }
        }
    }
}
