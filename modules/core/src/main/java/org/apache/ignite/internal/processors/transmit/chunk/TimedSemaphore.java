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

package org.apache.ignite.internal.processors.transmit.chunk;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;

/**
 * The semaphore which releases the acquired permits when the configured period of time ends.
 */
public final class TimedSemaphore {
    /** The constant which represents unlimited number of permits being acquired. */
    public static final int UNLIMITED_PERMITS = -1;

    /** The service to release permits during the configured period of time. */
    private final ScheduledExecutorService scheduler;

    /** The period time. */
    private final long period;

    /** The period unit. */
    private final TimeUnit unit;

    /** A future object representing the timer task. */
    private ScheduledFuture<?> timerFut;

    /** The maximum number of permits. */
    private int limit;

    /** The current acquired permits during the configured period of time. */
    private int acquireCnt;

    /** If the semaphore has been shutdowned. */
    private boolean shutdown;

    /**
     * @param period The time period.
     * @param unit The unit for the period.
     * @param limit The limit of permits.
     */
    public TimedSemaphore(
        long period,
        TimeUnit unit,
        int limit
    ) {
        assert period > 0;
        assert limit >= 0;

        this.period = period;
        this.unit = unit;
        this.limit = limit;

        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        this.scheduler = scheduler;
    }

    /**
     * @return The maximum number of available permits.
     */
    public synchronized int limit() {
        return limit;
    }

    /**
     * Sets the maximum number of available permits. In the other words, the number of times the
     * {@link #acquire()} method can be called within the given time period without being blocked.
     * To disbale the limit, set {@link #UNLIMITED_PERMITS}.
     *
     * @param limit The maximum number of available permits.
     */
    public synchronized void limit(final int limit) {
        this.limit = limit;
    }

    /**
     * Stop the current semaphore.
     */
    public synchronized void shutdown() {
        if (!shutdown) {
            shutdown = true;

            scheduler.shutdownNow();
        }
    }

    /**
     * This method will be blocked if the limit for the current period has been reached.
     * At the first call of this method the timer will be started to monitor (and release
     * if need) permits during current period.
     *
     * @throws InterruptedException if the thread gets interrupted
     */
    public synchronized void acquire() throws InterruptedException {
        initTimePeriod();

        boolean acquired;

        do {
            acquired = acquirePermit();

            if (!acquired)
                wait();
        }
        while (!acquired);
    }

    /**
     * Reset the permits counter and releases all the threads waiting for it.
     */
    private synchronized void release() {
        acquireCnt = 0;

        notifyAll();
    }

    /**
     * @return The current number of acquired permits during this period.
     */
    public synchronized int acquireCnt() {
        return acquireCnt;
    }

    /**
     * @return The current number of available permits during the current period
     * or {@link #UNLIMITED_PERMITS} if there is to permits limit.
     */
    public synchronized int availablePermits() {
        return limit == UNLIMITED_PERMITS ? limit : limit - acquireCnt;
    }

    /**
     * @return The future represens scheduled period timer.
     */
    synchronized ScheduledFuture<?> scheduleTimePeriod() {
        // The time period ends - release all permits.
        return scheduler.scheduleAtFixedRate(this::release, period, period, unit);
    }

    /**
     * Checks if the semaphore can be used and starts the internal process for period monitoring.
     */
    private void initTimePeriod() {
        if (shutdown)
            throw new IgniteException("The semaphore has been shutdown.");

        if (timerFut == null)
            timerFut = scheduleTimePeriod();
    }

    /**
     * @return {@code true} if permit has been successfully acquired.
     */
    private boolean acquirePermit() {
        if (acquireCnt < limit || UNLIMITED_PERMITS == limit) {
            acquireCnt++;

            return true;
        }

        return false;
    }
}