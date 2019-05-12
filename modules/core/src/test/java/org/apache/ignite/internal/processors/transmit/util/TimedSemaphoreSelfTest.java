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
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TimedSemaphoreSelfTest {
    /** */
    private TimedSemaphore semaphore;

    /** */
    @After
    public void afterCleanup() throws Exception {
        semaphore.shutdown();
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
            latch,
            acquireRepeats,
            permitsPerAcquire);

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
            threads[i] = new SemaphoreTestThread(semaphore, latch, acquireRepeats, permitsPerAcquire);
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
            latch,
            acquireRepeats,
            permitsPerAcquire);

        thread.start();

        assertTrue(latch.await(permitsPerAcquire, TimeUnit.SECONDS));

        thread.join();

        // The number of periods (except the initial period) to release permits must be equal
        // to permitsPerAcquire, as we configure onle single available permit per time period.
        assertEquals(permitsPerAcquire, periodEndCount.get() + 1);
    }

    /**
     *
     */
    private static class SemaphoreTestThread extends Thread {
        /** The testing instance of semaphore. */
        private final TimedSemaphore semaphore;

        /** */
        private final CountDownLatch successAcquireLatch;

        /** The total number of repeats with calling {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method. */
        private final int repeats;

        /** The number of permits to acquire per single {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method call. */
        private final int singleAcquirePermits;

        /**
         * @param semaphore The semaphore to test.
         * @param successAcquireLatch The latch from the main thread.
         * @param repeats The total number of repeats with calling
         * {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method.
         * @param singleAcquirePermits The number of permits to acquire per single
         * {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method call.
         */
        SemaphoreTestThread(
            TimedSemaphore semaphore,
            CountDownLatch successAcquireLatch,
            int repeats,
            int singleAcquirePermits
        ) {
            this.semaphore = semaphore;
            this.successAcquireLatch = successAcquireLatch;
            this.repeats = repeats;
            this.singleAcquirePermits = singleAcquirePermits;
        }

        /** */
        @Override public void run() {
            try {
                for (int i = 0; i < repeats; i++) {
                    semaphore.tryAcquire(singleAcquirePermits, 0, TimeUnit.SECONDS);

                    successAcquireLatch.countDown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
