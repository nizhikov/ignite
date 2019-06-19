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

package org.apache.ignite.internal.processors.jobmetrics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.ACTIVE;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.CANCELED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.FINISHED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.JOBS;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.REJECTED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.STARTED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.WAITING;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Grid job metrics processor load test.
 */
public class GridJobMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    public static final long TIMEOUT = 10_000;

    /** */
    private static final int THREADS_CNT = 10;

    /** */
    private GridTestKernalContext ctx;

    /** */
    private static CountDownLatch latch;

    /** */
    public GridJobMetricsSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();

        ctx.add(new GridResourceProcessor(ctx));
        ctx.add(new GridJobMetricsProcessor(ctx));

        ctx.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ctx.stop(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testJobMetricsMultiThreaded() throws Exception {
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    int i = 0;
                    while (i++ < 1000)
                        ctx.jobMetric().addSnapshot(new GridJobMetricsSnapshot());
                }
                catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }, THREADS_CNT, "grid-job-metrics-test");

        ctx.jobMetric().getJobMetrics();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    int i = 0;
                    while (i++ < 100000)
                        ctx.jobMetric().addSnapshot(new GridJobMetricsSnapshot());
                }
                catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }, THREADS_CNT, "grid-job-metrics-test");

        ctx.jobMetric().getJobMetrics();
    }

    @Test
    public void testJobMetrics() throws Exception {
        ctx.jobMetric().reset();

        final int waitTime = 1;
        final int activeJobs = 2;
        final int passiveJobs = 3;
        final int cancelJobs = 4;
        final int rejectJobs = 5;
        final int execTime = 6;
        final int startedJobs = 7;
        final int maxExecTime = 8;
        final int maxWaitTime = 9;
        final int finishedJobs = 10;
        final double cpuLoad = 11.0;

        GridJobMetricsSnapshot s = new GridJobMetricsSnapshot();

        s.setWaitTime(waitTime);
        s.setStartedJobs(startedJobs);

        s.setExecutionTime(execTime);
        s.setFinishedJobs(finishedJobs);

        s.setActiveJobs(activeJobs);
        s.setPassiveJobs(passiveJobs);
        s.setCancelJobs(cancelJobs);
        s.setRejectJobs(rejectJobs);
        s.setMaximumExecutionTime(maxExecTime);
        s.setMaximumWaitTime(maxWaitTime);
        s.setCpuLoad(cpuLoad);

        int cnt = 3;

        for(int i=0; i<cnt; i++)
            ctx.jobMetric().addSnapshot(s);

        GridJobMetrics m = ctx.jobMetric().getJobMetrics();

        assertEquals(activeJobs, m.getMaximumActiveJobs());
        assertEquals(activeJobs, m.getCurrentActiveJobs());
        assertEquals(activeJobs, m.getAverageActiveJobs());

        assertEquals(passiveJobs, m.getMaximumWaitingJobs());
        assertEquals(passiveJobs, m.getCurrentWaitingJobs());
        assertEquals(passiveJobs, m.getAverageWaitingJobs());

        assertEquals(cancelJobs, m.getMaximumCancelledJobs());
        assertEquals(cancelJobs, m.getCurrentCancelledJobs());
        assertEquals(cancelJobs, m.getAverageCancelledJobs());

        assertEquals(rejectJobs, m.getMaximumRejectedJobs());
        assertEquals(rejectJobs, m.getCurrentRejectedJobs());
        assertEquals(rejectJobs, m.getAverageRejectedJobs());

        assertEquals(maxExecTime, m.getMaximumJobExecuteTime());
        assertEquals(maxExecTime, m.getCurrentJobExecuteTime());
        assertEquals(1.0*execTime/ finishedJobs, m.getAverageJobExecuteTime());

        assertEquals(maxWaitTime, m.getMaximumJobWaitTime());
        assertEquals(maxWaitTime, m.getCurrentJobWaitTime());
        assertEquals(1.0*waitTime/ startedJobs, m.getAverageJobWaitTime());

        assertEquals(cnt* finishedJobs, m.getTotalExecutedJobs());
        assertEquals(cnt* cancelJobs, m.getTotalCancelledJobs());
        assertEquals(cnt* rejectJobs, m.getTotalRejectedJobs());

        assertEquals(cpuLoad, m.getAverageCpuLoad());

        assertEquals(cnt* execTime, m.getTotalJobsExecutionTime());
        assertEquals(0, m.getCurrentIdleTime());
    }

    @Test
    public void testGridJobWaitingRejectedMetrics() throws Exception {
        latch = new CountDownLatch(1);

        GridTestCollision collisioinSpi = new GridTestCollision();

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setCollisionSpi(collisioinSpi);

        try (IgniteEx g = startGrid(cfg)) {
            ReadOnlyMetricRegistry mreg = g.context().metric().registry().withPrefix(JOBS);

            LongMetric started = (LongMetric)mreg.findMetric(STARTED);
            LongMetric active = (LongMetric)mreg.findMetric(ACTIVE);
            LongMetric waiting = (LongMetric)mreg.findMetric(WAITING);
            LongMetric canceled = (LongMetric)mreg.findMetric(CANCELED);
            LongMetric rejected = (LongMetric)mreg.findMetric(REJECTED);
            LongMetric finished = (LongMetric)mreg.findMetric(FINISHED);

            assertNotNull(started);
            assertNotNull(active);
            assertNotNull(waiting);
            assertNotNull(canceled);
            assertNotNull(rejected);
            assertNotNull(finished);

            assertEquals(0, started.value());
            assertEquals(0, active.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(0, finished.value());

            SimplestTask task1 = new SimplestTask();
            SimplestTask task2 = new SimplestTask();
            SimplestTask task3 = new SimplestTask();

            task1.block = true;
            task2.block = true;
            task3.block = true;

            //Task will become "waiting", because of CollisionSpi implementation.
            ComputeTaskFuture<?> fut1 = g.compute().executeAsync(task1, 1);
            ComputeTaskFuture<?> fut2 = g.compute().executeAsync(task2, 1);
            ComputeTaskFuture<?> fut3 = g.compute().executeAsync(task3, 1);

            assertEquals(0, started.value());
            assertEquals(0, active.value());
            assertEquals(3, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(0, finished.value());

            //Activating 2 of 3 jobs. Rejecting 1 of them.
            Iterator<CollisionJobContext> iter = collisioinSpi.jobs.values().iterator();

            iter.next().cancel();

            assertEquals(1, rejected.value());

            iter.next().activate();
            iter.next().activate();

            latch.countDown();

            boolean res = waitForCondition(() -> fut1.isDone() && fut2.isDone() && fut3.isDone(), TIMEOUT);

            assertTrue(res);

            res = waitForCondition(() -> finished.value() == 3, TIMEOUT);

            assertTrue(res);
        }
    }

    @Test
    public void testGridJobMetrics() throws Exception {
        latch = new CountDownLatch(1);

        try(IgniteEx g = startGrid(0)) {
            ReadOnlyMetricRegistry mreg = g.context().metric().registry().withPrefix(JOBS);

            LongMetric started = (LongMetric)mreg.findMetric(STARTED);
            LongMetric active = (LongMetric)mreg.findMetric(ACTIVE);
            LongMetric waiting = (LongMetric)mreg.findMetric(WAITING);
            LongMetric canceled = (LongMetric)mreg.findMetric(CANCELED);
            LongMetric rejected = (LongMetric)mreg.findMetric(REJECTED);
            LongMetric finished = (LongMetric)mreg.findMetric(FINISHED);

            assertNotNull(started);
            assertNotNull(active);
            assertNotNull(waiting);
            assertNotNull(canceled);
            assertNotNull(rejected);
            assertNotNull(finished);

            assertEquals(0, started.value());
            assertEquals(0, active.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(0, finished.value());

            SimplestTask task = new SimplestTask();

            g.compute().execute(task, 1);

            boolean res = waitForCondition(() -> active.value() == 0, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(1, started.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(1, finished.value());

            task.block = true;

            ComputeTaskFuture<?> fut = g.compute().executeAsync(task, 1);

            res = waitForCondition(() -> active.value() == 1, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(2, started.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(1, finished.value());

            latch.countDown();

            fut.get(TIMEOUT);

            res = waitForCondition(() -> active.value() == 0, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(2, finished.value());

            latch = new CountDownLatch(1);

            fut = g.compute().executeAsync(task, 1);

            res = waitForCondition(() -> active.value() == 1, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(3, started.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(2, finished.value());

            fut.cancel();

            latch.countDown();

            res = waitForCondition(() -> active.value() == 0, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(3, started.value());
            assertEquals(0, waiting.value());
            assertEquals(1, canceled.value());
            assertEquals(0, rejected.value());

            res = waitForCondition(() -> finished.value() == 3, TIMEOUT);

            assertTrue("Finished = " + finished.value(), res);
        }
    }

    /** */
    private static class SimplestJob implements ComputeJob {
        boolean block;

        public SimplestJob(boolean block) {
            this.block = block;
        }

        /** */
        @Override public void cancel() {
            // No-op.
        }

        /** */
        @Override public Object execute() throws IgniteException {
            if (block) {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }

            return "1";
        }
    }

    /** */
    private static class SimplestTask extends ComputeTaskAdapter<Object, Object> {
        /** */
        boolean block;

        /** */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid)
                jobs.put(new SimplestJob(block), node);

            return jobs;
        }

        /** */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return "1";
        }
    }

    /** */
    @IgniteSpiMultipleInstancesSupport(true)
    public static class GridTestCollision extends IgniteSpiAdapter implements CollisionSpi {
        /** */
        HashMap<ComputeJob, CollisionJobContext> jobs = new HashMap<>();

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            for (CollisionJobContext jobCtx : ctx.waitingJobs())
                jobs.put(jobCtx.getJob(), jobCtx);

        }

        /** {@inheritDoc} */
        @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            // No-op.
        }
    }
}
