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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
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

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;

/**
 *
 */
public class IgniteFileTransmitProcessorSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long CACHE_SIZE = 50_000L;

    /** */
    private static final String TEMP_FILES_DIR = "ctmp";

    /** The temporary directory to store files. */
    private File tempStore;

    /**
     * @throws Exception if failed.
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();

        tempStore = U.resolveWorkDirectory(U.defaultWorkDirectory(), TEMP_FILES_DIR, true);
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
     * @throws Exception If fails.
     */
    @Test
    public void testTransmitCachePartitionsToTopic() throws Exception {
        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);

        ig0.cluster().active(true);

        addCacheData(ig0, DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Object topic = GridTopic.TOPIC_CACHE.topic("test", 0);

        ConcurrentMap<String, Long> fileWithSizes = new ConcurrentHashMap<>();

        ig1.context().fileTransmit().addFileIoChannelHandler(topic, new TransmitSessionFactory() {
            @Override public TransmitSession create() {
                return new TransmitSession() {
                    @Override public void begin(UUID nodeId, String sessionId) {
                        assertTrue(ig0.localNode().id().equals(nodeId));
                    }

                    @Override public ChunkHandler chunkHandler() {
                        return null;
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

                    @Override public void end() {
                        // No-op.
                    }

                    @Override public void onException(Throwable cause) {
                        // No-op.
                    }
                };
            }
        });

        File cacheDirIg0 = cacheWorkDir(ig0, DEFAULT_CACHE_NAME);

        try (FileWriter writer = ig0.context()
            .fileTransmit()
            .fileWriter(ig1.localNode().id(), topic, (byte)1)) {
            // Iterate over cache partition files.
            File [] files = cacheDirIg0.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.endsWith(FILE_SUFFIX);
                }
            });

            for (File file : files)
                fileWithSizes.put(file.getName(), file.length());

            for (File file : files)
                writer.write(file, 0, file.length(), new HashMap<>(), ReadPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testReconnectWhenChannelClosed() throws Exception {

    }
}
