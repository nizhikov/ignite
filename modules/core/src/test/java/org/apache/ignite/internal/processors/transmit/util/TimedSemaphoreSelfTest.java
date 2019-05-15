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

package org.apache.ignite.internal.processors.transmit.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TimedSemaphoreSelfTest {
    /** */
    private TimedSemaphore semaphore;

    /** */
    @Before
    public void beforeTest() throws Exception {
        U.onGridStart();
    }

    /** */
    @After
    public void afterCleanup() throws Exception {
        semaphore.shutdown();
        U.onGridStop();
    }

    /** */
    @Test
    public void testAcquirePermits() throws Exception {
        final int acquireRepeats = 5;
        final int permitsPerAcquire = 1;
        final int totalPermits = 4;
        final CountDownLatch latch = new CountDownLatch(totalPermits);
        final AtomicBoolean canReleasePermits = new AtomicBoolean();

        semaphore = new TimedSemaphore(totalPermits) {
            @Override synchronized void release() {
                // Skip the period release pertmis until all checks will be completed.
                if (canReleasePermits.get())
                    super.release();
            }
        };

        final SemaphoreTestThread thread = new SemaphoreTestThread(semaphore,
            acquireRepeats,
            permitsPerAcquire,
            0,
            res -> {
                if (res)
                    latch.countDown();
            });

        thread.start();
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        // All the semaphore's permits must be aquired and the thread blocked
        assertEquals(totalPermits, semaphore.acquireCnt());

        canReleasePermits.set(true);
        thread.join();

        // Wait for the time period end, all the acuired permits must be released.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return semaphore.acquireCnt() == 0;
            }
        }, 2_000);
    }

    /** */
    @Test
    public void testAcquirePermitsMutlithreaded() throws Exception {
        final int acquireRepeats = 1;
        final int permitsPerAcquire = 1;
        final int totalPermits = 2;
        final int threadsCnt = 4;
        final CountDownLatch latch = new CountDownLatch(threadsCnt);

        semaphore = new TimedSemaphore(totalPermits);

        final SemaphoreTestThread[] threads = new SemaphoreTestThread[threadsCnt];

        for (int i = 0; i < threadsCnt; i++) {
            threads[i] = new SemaphoreTestThread(semaphore,
                acquireRepeats,
                permitsPerAcquire,
                0,
                res -> {
                    if (res)
                        latch.countDown();
                });
            threads[i].start();
        }

        assertTrue(latch.await(threadsCnt, TimeUnit.SECONDS));

        for (int i = 0; i < threadsCnt; i++)
            threads[i].join();

        // At the end all acquired permits must be released.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return semaphore.acquireCnt() == 0;
            }
        }, 2_000);
    }

    /** */
    @Test
    public void testAcquireBatchPermits() throws Exception {
        final int acquireRepeats = 1;
        final int permitsPerAcquire = 4;
        final int totalPermits = 1;
        final AtomicInteger periodEndCount = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(acquireRepeats);

        semaphore = new TimedSemaphore(totalPermits) {
            @Override synchronized void release() {
                // Count periods only if any permit acquired.
                if (acquireCnt() != 0)
                    periodEndCount.incrementAndGet();

                super.release();
            }
        };

        final SemaphoreTestThread thread = new SemaphoreTestThread(semaphore,
            acquireRepeats,
            permitsPerAcquire,
            0,
            res -> {
                if (res)
                    latch.countDown();
            });

        thread.start();

        assertTrue(latch.await(permitsPerAcquire, TimeUnit.SECONDS));

        thread.join();

        // The number of periods (except the initial period) to release permits must be equal
        // to permitsPerAcquire, as we configure onle single available permit per time period.
        assertEquals(permitsPerAcquire, periodEndCount.get() + 1);
    }

    /** */
    @Test
    public void testAcquirePermitsWithTimeout() throws Exception {
        final int acquireRepeats = 1;
        final int permitsPerAcquire = 10;
        final int totalPermits = 1;
        final int timeoutSec = 2;

        semaphore = new TimedSemaphore(totalPermits);

        final SemaphoreTestThread thread = new SemaphoreTestThread(semaphore,
            acquireRepeats,
            permitsPerAcquire,
            timeoutSec,
            res -> {
                assertFalse("tryAcquire must fail due to wait period ended", res);
            });

        thread.start();
        thread.join();
    }

    /**
     *
     */
    private static class SemaphoreTestThread extends Thread {
        /** The testing instance of semaphore. */
        private final TimedSemaphore semaphore;

        /** The total number of repeats with calling {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method. */
        private final int repeats;

        /** The number of permits to acquire per single {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method call. */
        private final int singleAcquirePermits;

        /** Timeout in seconds to wait permits. */
        private final int timeoutSec;

        /** Permits acquire result consumer. */
        private final Consumer<Boolean> onAcquire;

        /**
         * @param semaphore The semaphore to test.
         * @param repeats The total number of repeats with calling
         * {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method.
         * @param singleAcquirePermits The number of permits to acquire per single
         * {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method call.
         * @param timeoutSec Time in seconds to wait permits.
         * @param onAcquire Permits acquire result consumer.
         */
        SemaphoreTestThread(
            TimedSemaphore semaphore,
            int repeats,
            int singleAcquirePermits,
            int timeoutSec,
            Consumer<Boolean> onAcquire
        ) {
            this.semaphore = semaphore;
            this.repeats = repeats;
            this.singleAcquirePermits = singleAcquirePermits;
            this.timeoutSec = timeoutSec;
            this.onAcquire = onAcquire;
        }

        /** */
        @Override public void run() {
            try {
                for (int i = 0; i < repeats; i++) {
                    if (semaphore.tryAcquire(singleAcquirePermits, timeoutSec, TimeUnit.SECONDS))
                        onAcquire.accept(true);
                    else
                        onAcquire.accept(false);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
