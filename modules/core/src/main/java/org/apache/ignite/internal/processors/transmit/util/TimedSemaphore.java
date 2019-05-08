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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;

/**
 * The semaphore which releases the acquired permits when the configured period of time ends.
 */
public class TimedSemaphore {
    /** The constant which represents unlimited number of permits being acquired. */
    public static final int UNLIMITED_PERMITS = -1;

    /** The amount of time perdiod. */
    private static final int DFLT_TIME_PERIOD_AMOUNT = 1;

    /** The time period unit. */
    private static final TimeUnit DFLT_TIME_PERIOD_UNIT = TimeUnit.SECONDS;

    /** The service to release permits during the configured time of time. */
    private final ScheduledExecutorService scheduler;

    /** A future object representing the timer task. */
    private ScheduledFuture<?> timerFut;

    /** The maximum number of permits available per second. */
    private int permitsPerSec;

    /** The current acquired permits during the configured period of time. */
    private int acquireCnt;

    /** If the semaphore has been shutdowned. */
    private boolean shutdown;

    /**
     * @param permitsPerSec The number of permits per second.
     */
    public TimedSemaphore(int permitsPerSec) {
        assert permitsPerSec >= 0;

        this.permitsPerSec = permitsPerSec;

        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        this.scheduler = scheduler;
    }

    /**
     * @return The maximum number of available permits.
     */
    public synchronized int permitsPerSec() {
        return permitsPerSec;
    }

    /**
     * Sets the maximum number of available permits. In the other words, the number of times the
     * {@link ##acquire(int)()} method can be called within the given time time without being blocked.
     * To disbale the limit, set {@link #UNLIMITED_PERMITS}.
     *
     * @param permitsPerSec The maximum number of available permits per second.
     */
    public synchronized void permitsPerSec(final int permitsPerSec) {
        this.permitsPerSec = permitsPerSec;
    }

    /**
     * Stop the current semaphore.
     */
    public synchronized void shutdown() {
        if (!shutdown) {
            shutdown = true;

            scheduler.shutdownNow();

            timerFut = null;
        }
    }

    /**
     * This method will be blocked if the limit of permits for the current time period has been reached.
     * At the first call of this method the timer will be started to monitor permits during current period.
     *
     * @param permits The total number of permits to acquire.
     * @throws InterruptedException If the thread gets interrupted.
     */
    public synchronized void acquire(final int permits) throws InterruptedException {
        assert permits > 0;

        initTimePeriod();

        for (int i = 0; i < permits; ) {
            if (acquirePermit())
                i++;
            else
                wait();
        }
    }

    /**
     * @return {@code true} if permit has been successfully acquired.
     */
    private boolean acquirePermit() {
        if (acquireCnt < permitsPerSec || UNLIMITED_PERMITS == permitsPerSec) {
            acquireCnt++;

            return true;
        }

        return false;
    }

    /**
     * Reset the permits counter and releases all the threads waiting for it.
     */
    synchronized void release() {
        acquireCnt = 0;

        notifyAll();
    }

    /**
     * @return The current number of acquired permits during this time.
     */
    public synchronized int acquireCnt() {
        return acquireCnt;
    }

    /**
     * @return The current number of available permits during the current time
     * or {@link #UNLIMITED_PERMITS} if there is to permits limit.
     */
    public synchronized int availablePermits() {
        return permitsPerSec == UNLIMITED_PERMITS ? permitsPerSec : permitsPerSec - acquireCnt;
    }

    /**
     * @return The future represens scheduled time timer.
     */
    synchronized ScheduledFuture<?> scheduleTimePeriod() {
        // The time time ends - release all permits.
        return scheduler.scheduleAtFixedRate(this::release,
            DFLT_TIME_PERIOD_AMOUNT,
            DFLT_TIME_PERIOD_AMOUNT,
            DFLT_TIME_PERIOD_UNIT);
    }

    /**
     * Checks if the semaphore can be used and starts the internal process for time monitoring.
     */
    private void initTimePeriod() {
        if (shutdown)
            throw new IgniteException("The semaphore has been shutdowned.");

        if (timerFut == null)
            timerFut = scheduleTimePeriod();
    }
}
