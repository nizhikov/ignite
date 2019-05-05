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

package org.apache.ignite.internal.processors.transmit;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;

/**
 *
 */
public class IgniteFileTransmitProcessorSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long CACHE_SIZE = 50_000L;

    /** */
    private static final int FILE_SIZE_BYTES = 50 * 1024 * 1024;

    /** */
    private static final String TEMP_FILES_DIR = "ctmp";

    /** The temporary directory to store files. */
    private File tempStore;

    /** The topic to send files to. */
    private Object topic;

    /** File filter. */
    private FilenameFilter fileBinFilter;

    /**
     * @throws Exception if failed.
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();

        tempStore = U.resolveWorkDirectory(U.defaultWorkDirectory(), TEMP_FILES_DIR, true);
        topic = GridTopic.TOPIC_CACHE.topic("test", 0);
        fileBinFilter = new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(FILE_SUFFIX);
            }
        };
    }

    /**
     * @throws Exception if failed.
     */
    @After
    public void after() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(500L * 1024 * 1024)))
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(8)));

    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void addCacheData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer<Integer, Integer> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.allowOverwrite(true);

            for (int i = 0; i < CACHE_SIZE; i++) {
                if ((i + 1) % (CACHE_SIZE / 10) == 0)
                    log.info("Prepared " + (i + 1) * 100 / (CACHE_SIZE) + "% entries.");

                dataStreamer.addData(i, i + cacheName.hashCode());
            }
        }
    }

    /**
     * @param ignite The ignite instance.
     * @param cacheName Cache name string representation.
     * @return The cache working directory.
     */
    private File cacheWorkDir(IgniteEx ignite, String cacheName) {
        // Resolve cache directory
        IgniteInternalCache<?, ?> cache = ignite.cachex(cacheName);

        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)cache.context()
            .shared()
            .pageStore();

        return pageStoreMgr.cacheWorkDir(cache.configuration());
    }

    /**
     * @param name The file name to create.
     * @param size The file size.
     * @throws IOException If fails.
     */
    private File createFileRandomData(String name, final int size) throws IOException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        File out = new File(tempStore, name);

        try (RandomAccessFile raf = new RandomAccessFile(out, "rw")) {
            byte[] buf = new byte[size];
            rnd.nextBytes(buf);
            raf.write(buf);
        }

        return out;
    }


    /**
     * @throws Exception If fails.
     */
    @Test
    public void testTransmitCachePartitionsToTopic() throws Exception {
        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        addCacheData(sender, DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        ConcurrentMap<String, Long> fileWithSizes = new ConcurrentHashMap<>();

        receiver.context().fileTransmit().addFileIoChannelHandler(topic, new TransmitSessionFactory() {
            @Override public TransmitSession create() {
                return new TransmitSessionAdapter() {
                    @Override public void begin(UUID nodeId, String sessionId) {
                        assertTrue(sender.localNode().id().equals(nodeId));
                    }

                    @Override public FileHandler fileHandler() {
                        return new FileHandler() {
                            @Override public String begin(
                                String name,
                                long position,
                                long count,
                                Map<String, Serializable> params
                            ) {
                                return new File(tempStore, name).getAbsolutePath();
                            }

                            @Override public void end(File file, Map<String, Serializable> params) {
                                assertTrue(fileWithSizes.containsKey(file.getName()));
                                assertEquals(fileWithSizes.get(file.getName()), new Long(file.length()));
                            }
                        };
                    }
                };
            }
        });

        File cacheDirIg0 = cacheWorkDir(sender, DEFAULT_CACHE_NAME);

        try (FileWriter writer = sender.context()
            .fileTransmit()
            .fileWriter(receiver.localNode().id(), topic, PUBLIC_POOL)) {
            // Iterate over cache partition files.
            File [] files = cacheDirIg0.listFiles(fileBinFilter);

            for (File file : files)
                fileWithSizes.put(file.getName(), file.length());

            for (File file : files)
                writer.write(file, 0, file.length(), new HashMap<>(), ReadPolicy.FILE);
        }

        stopAllGrids();

        assertEquals(fileWithSizes.size(), tempStore.listFiles(fileBinFilter).length);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testReconnectWhenChannelClosed() throws Exception {
        final AtomicBoolean failFirstTime = new AtomicBoolean();

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("50Mb", FILE_SIZE_BYTES);

        receiver.context().fileTransmit().addFileIoChannelHandler(topic, new TransmitSessionFactory() {
            @Override public TransmitSession create() {
                return new TransmitSessionAdapter() {
                    @Override public void begin(UUID nodeId, String sessionId) {
                        if (failFirstTime.compareAndSet(false, true))
                            throw new IgniteException("Session initialization failed. Connection must be reestablished.");
                    }

                    @Override public FileHandler fileHandler() {
                        return new FileHandler() {
                            @Override public String begin(
                                String name,
                                long position,
                                long count,
                                Map<String, Serializable> params
                            ) {
                                return new File(tempStore, name + "_" + receiver.localNode().id()).getAbsolutePath();
                            }

                            @Override public void end(File file, Map<String, Serializable> params) {
                                assertEquals(fileToSend.length(), file.length());
                            }
                        };
                    }
                };
            }
        });

        try (FileWriter writer = sender.context()
            .fileTransmit()
            .fileWriter(receiver.localNode().id(), topic, PUBLIC_POOL)) {
            writer.write(fileToSend, 0, fileToSend.length(), new HashMap<>(), ReadPolicy.FILE);
        }
    }

    /**
     * The defailt implementation of transmit session.
     */
    private static class TransmitSessionAdapter implements TransmitSession {
        /** {@inheritDoc} */
        @Override public void begin(UUID nodeId, String sessionId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public ChunkHandler chunkHandler() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FileHandler fileHandler() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void end() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onException(Throwable cause) {
            // No-op.
        }
    }
}
