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

package org.apache.ignite.internal.managers.communication;

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;

/**
 * Test for {@link GridIoManager}.
 */
public class GridIoManagerChannelSelfTest extends GridCommonAbstractTest {
    /** Default IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** The number of test nodes. */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TcpCommunicationSpi());
        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOpenChannelToRemoteTopic() throws Exception {
        startGrids(NODES_CNT);

        final SocketChannel[] nioCh = new SocketChannel[1];
        final CountDownLatch waitChLatch = new CountDownLatch(1);
        final Object topic = TOPIC_CACHE.topic("channel", 0);

        grid(1).context().io().addChannelListener(topic, new GridChannelListener() {
            @Override public void onChannelOpened(UUID nodeId, Message initMsg, Channel channel) {
                // Created from ignite node with index = 0;
                if (nodeId.equals(grid(0).localNode().id())) {
                    nioCh[0] = (SocketChannel)channel;

                    waitChLatch.countDown();
                }
            }
        });

        GridIoManager ioMgr = grid(0).context().io();

        SocketChannel nioChannel = (SocketChannel)ioMgr.openChannel(grid(1).localNode().id(), topic,
            new AffinityTopologyVersion(1))
            .get(10, TimeUnit.SECONDS);

        // Wait for the channel connection established.
        assertTrue(waitChLatch.await(5_000L, TimeUnit.MILLISECONDS));

        assertNotNull(nioCh[0]);

        // Prepare ping bytes to check connection.
        final int pingNum = 777_777;
        final int pingBuffSize = 4;

        ByteBuffer writeBuf = ByteBuffer.allocate(pingBuffSize);

        writeBuf.putInt(pingNum);
        writeBuf.flip();

        // Write ping bytes to the channel.
        int cnt = nioChannel.write(writeBuf);

        assertEquals(pingBuffSize, cnt);

        // Read test bytes from channel on remote node.
        ReadableByteChannel readCh = nioCh[0];

        ByteBuffer readBuf = ByteBuffer.allocate(pingBuffSize);

        for (int i = 0; i < pingBuffSize; ) {
            int read = readCh.read(readBuf);

            if (read == -1)
                throw new IgniteException("Failed to read remote node ID");

            i += read;
        }

        readBuf.flip();

        // Check established channel.
        assertEquals(pingNum, readBuf.getInt());
    }
}
