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

package org.apache.ignite.internal.processors.service;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridServiceClientNodeTest extends GridCommonAbstractTest {
    /** */
    public static final int CNT = 800;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientFailureDetectionTimeout(30000);
        cfg.setMetricsUpdateFrequency(1000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployFromClient() throws Exception {
        startGrids(3);

        Ignite ignite = startClientGrid(3);

        checkDeploy(ignite, "service1");
    }

    /** @throws Exception If failed. */
    @Test
    public void testDeployFromClientAndCallArrayArgs() throws Exception {
        startGrids(1);

        Ignite client = startClientGrid(1);

        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setName("arrayArgMethodSvc");
        svcCfg.setService(new ArrayArgMethodServiceImpl());
        svcCfg.setMaxPerNodeCount(1);

        client.services().deploy(svcCfg);

        ArrayArgMethodService svc =
            client.services().serviceProxy("arrayArgMethodSvc", ArrayArgMethodService.class, true);

        assertNotNull(svc);

        Param[] params = new Param[CNT];

        for (int i = 0; i < CNT; i++) {
            params[i] = new Param(i);
            params[i].setVals(new ParamVal[CNT]);

            for (int j = 0; j < CNT; j++)
                params[i].getVals()[j] = new ParamVal(j, j * 42.0);
        }

        svc.method(params);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployFromClientAfterRouterStop1() throws Exception {
        startGrid(0);

        Ignite ignite = startClientGrid(1);

        startGrid(2);

        U.sleep(1000);

        stopGrid(0);

        awaitPartitionMapExchange();

        checkDeploy(ignite, "service1");

        startGrid(3);

        for (int i = 0; i < 10; i++)
            checkDeploy(ignite, "service2-" + i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployFromClientAfterRouterStop2() throws Exception {
        startGrid(0);

        Ignite ignite = startClientGrid(1);

        startGrid(2);

        startClientGrid(3);

        startGrid(4);

        U.sleep(1000);

        stopGrid(0);

        awaitPartitionMapExchange();

        checkDeploy(ignite, "service1");

        startGrid(5);

        for (int i = 0; i < 10; i++)
            checkDeploy(ignite, "service2-" + i);
    }

    /**
     * @param client Client node.
     * @param svcName Service name.
     * @throws Exception If failed.
     */
    private void checkDeploy(Ignite client, String svcName) throws Exception {
        assertTrue(client.configuration().isClientMode());

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(svcName, latch);

        client.services().deployClusterSingleton(svcName, new DummyService());

        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
    }

    /** */
    public interface ArrayArgMethodService {
        /** */
        void method(Param[] params);
    }

    /** */
    public static class ArrayArgMethodServiceImpl implements ArrayArgMethodService, Service {
        /** {@inheritDoc} */
        @Override public void method(Param[] params) {
            assertNotNull(params);

            assertEquals(CNT, params.length);

            for (Param param : params) {
                assertNotNull(param);

                assertNotNull(param.getVals());

                assertEquals(CNT, param.getVals().length);
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }

    /** */
    public static class Param implements Serializable {
        /** */
        int id;

        /** */
        ParamVal[] vals;

        public Param(int id) {
            this.id = id;
        }

        /** */
        public ParamVal[] getVals() {
            return vals;
        }

        /** */
        public void setVals(ParamVal[] vals) {
            this.vals = vals;
        }
    }

    /** */
    public static class ParamVal implements Serializable {
        /** */
        int id;

        /** */
        double val;

        public ParamVal(int id, double val) {
            this.id = id;
            this.val = val;
        }
    }
}
