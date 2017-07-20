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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/**
 *
 */
public class IgniteOptimisticTxSuspendResumeFailoverTest extends AbstractTransactionsInMultipleThreadsTest {
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Starts tx locally with locally residing keys and then local node fails.
     */
    public void testTxLocalNodeFailover() throws Exception {
        final IgniteEx remoteNode = startGrid(DEFAULT_NODE_ID);

        final IgniteCache<Integer, String> remoteCache = remoteNode.cache(DEFAULT_CACHE_NAME);

        runWithAllIsolations(new CI1Exc<TransactionIsolation>() {
            @Override public void applyX(TransactionIsolation isolation) throws Exception {
                Ignite clientNode = startGrid(getTestIgniteInstanceName(CLIENT_NODE_ID));

                awaitPartitionMapExchange();

                try {
                    performTxFailover(clientNode, clientNode, 1, isolation);
                }
                catch (IgniteCheckedException ignore) {
                    // ignoring node breakage exception.
                }

                assertFalse(remoteCache.containsKey(1));
            }
        });
    }

    /**
     * Starts transaction, breaks node and then resuming it in another thread.
     *
     * @param gridToBreak Node to brake.
     * @param gridToPerform Node, to start transaction on.
     * @param key Key to put.
     * @throws IgniteCheckedException If failed.
     */
    private void performTxFailover(Ignite gridToBreak, Ignite gridToPerform, int key, TransactionIsolation isolation)
        throws IgniteCheckedException {

        final IgniteTransactions txs = gridToBreak.transactions();

        IgniteCache<Integer, String> cache = gridToPerform.cache(DEFAULT_CACHE_NAME);

        final Transaction tx = txs.txStart(OPTIMISTIC, isolation);

        cache.put(key, "1");

        tx.suspend();

        G.stop(gridToBreak.name(), true);

        assertNull(txs.tx());

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                assertNull(txs.tx());

                assertEquals(SUSPENDED, tx.state());

                tx.resume();

                assertEquals(ACTIVE, tx.state());

                tx.commit();
            }
        }).get(5000);
    }
}
