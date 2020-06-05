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

package org.apache.ignite.internal.processors.security.events;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Test that an event's local listener and an event's remote filter get correct subjectId
 * when a server (client) node create or destroy a cache.
 */
@SuppressWarnings({"rawtypes", "unchecked", "ZeroLengthArrayAllocation"})
@RunWith(Parameterized.class)
public class CacheCreateDestroyEventsTest extends AbstractSecurityCacheEventTest {
    /** */
    @Parameterized.Parameter()
    public int cacheCnt;

    /** */
    @Parameterized.Parameter(1)
    public String evtNode;

    /** */
    @Parameterized.Parameter(2)
    public int evtType;

    /** */
    @Parameterized.Parameter(3)
    public int opNum;

    /** Parameters. */
    @Parameterized.Parameters(name = "cacheCnt={0},evtNode={1},evtType={2},opNum={3}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] {1, SRV, EVT_CACHE_STARTED, 1},
            new Object[] {1, CLNT, EVT_CACHE_STARTED, 1},
            new Object[] {1, SRV, EVT_CACHE_STARTED, 2},
            new Object[] {1, CLNT, EVT_CACHE_STARTED, 2},
            new Object[] {1, SRV, EVT_CACHE_STOPPED, 3},
            new Object[] {1, CLNT, EVT_CACHE_STOPPED, 3},
            new Object[] {2, SRV, EVT_CACHE_STARTED, 4},
            new Object[] {2, CLNT, EVT_CACHE_STARTED, 4},
            new Object[] {2, SRV, EVT_CACHE_STOPPED, 5},
            new Object[] {2, CLNT, EVT_CACHE_STOPPED, 5},
            new Object[] {2, "NEW_CLIENT_NODE", EVT_CACHE_STARTED, 6},
            new Object[] {2, "NEW_SERVER_NODE", EVT_CACHE_STARTED, 6}
        );
    }

    /** */
    private void testCacheEvents(int evtType, Consumer<Collection<CacheConfiguration>> op) throws Exception {
        testCacheEvents(2, CLNT, evtType, cacheConfigurations(1, evtType == EVT_CACHE_STOPPED), op);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(SRV);

        startClientAllowAll(CLNT);
    }

    /** */
    @Test
    public void testDynamicCreateDestroyCache()
        throws Exception {
        Consumer<Collection<CacheConfiguration>> op;

        if (opNum == 1)
            op = ccfgs -> grid(evtNode).getOrCreateCache(ccfgs.iterator().next());
        else if (opNum == 2)
            op = ccfgs -> grid(evtNode).createCache(ccfgs.iterator().next());
        else if (opNum == 3)
            op = ccfgs -> grid(evtNode).destroyCache(ccfgs.iterator().next().getName());
        else if (opNum == 4)
            op = ccfgs -> grid(evtNode).createCaches(ccfgs);
        else if (opNum == 5)
            op = ccfgs -> grid(evtNode).destroyCaches(ccfgs.stream().map(CacheConfiguration::getName).collect(Collectors.toSet()));
        else if (opNum == 6)
            op = this::startG;
        else
            throw new RuntimeException("!");

        int expTimes = cacheCnt*2 + ((!evtNode.equals(SRV) && evtType == EVT_CACHE_STARTED) ? cacheCnt : 0);

        testCacheEvents(expTimes, evtNode, evtType, cacheConfigurations(cacheCnt, evtType == EVT_CACHE_STOPPED), op);
    }

    public void startG(Collection<CacheConfiguration> ccfgs) {
        try {
            startGrid(getConfiguration(evtNode,
                new TestSecurityPluginProvider(evtNode, "", ALLOW_ALL, false))
                .setClientMode(evtNode.contains("CLIENT"))
                .setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[] {}))
            );
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private Consumer<Collection<CacheConfiguration>> operation(BiConsumer<IgniteClient, ClientCacheConfiguration> c) {
        return ccfgs -> {
            try (IgniteClient clnt = startClient()) {
                ClientCacheConfiguration ccfg = ccfgs.stream().findFirst()
                    .map(cfg -> new ClientCacheConfiguration().setName(cfg.getName()))
                    .orElseThrow(IllegalStateException::new);

                c.accept(clnt, ccfg);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    /**
     * @return Thin client for specified user.
     */
    private IgniteClient startClient() {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(CLNT)
                .setUserPassword("")
        );
    }
}
