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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.transmit.channel.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedFileStream;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedInputStream;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedStreamFactory;
import org.apache.ignite.internal.processors.transmit.util.ByteUnit;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
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
    private static final String TEMP_FILES_DIR = "ctmp";

    /** File io factory */
    private static final FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** The topic to send files to. */
    private static Object topic;

    /** File filter. */
    private static FilenameFilter fileBinFilter;

    /** The temporary directory to store files. */
    private File tempStore;

    /**
     * @throws Exception If fails.
     */
    @BeforeClass
    public static void beforeAll() throws Exception {
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
     * @param name The file name to create.
     * @param size The file size.
     * @throws IOException If fails.
     */
    private File createFileRandomData(String name, final int size, ByteUnit unit) throws IOException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        File out = new File(tempStore, name);
        int fileSize = (int)ByteUnit.BYTE.convertFrom(size, unit);

        try (RandomAccessFile raf = new RandomAccessFile(out, "rw")) {
            byte[] buf = new byte[fileSize];
            rnd.nextBytes(buf);
            raf.write(buf);
        }

        return out;
    }


    /**
     * Transmit all cache partition to particular topic on remote node.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerBase() throws Exception {
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
                            /** */
                            private final AtomicBoolean inited = new AtomicBoolean();

                            @Override public String begin(
                                String name,
                                long position,
                                long count,
                                Map<String, Serializable> params
                            ) {
                                assertTrue(inited.compareAndSet(false, true));

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

        log.info("Writing test files finished. All Ignite instances will be stopped.");

        stopAllGrids();

        assertEquals(fileWithSizes.size(), tempStore.listFiles(fileBinFilter).length);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testReconnectBeginSessionThrowsEx() throws Exception {
        final AtomicBoolean failFirstTime = new AtomicBoolean();
        final String exTestMessage = "Test exception. Session initialization failed. Connection will be reestablished.";

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("50Mb", 50, ByteUnit.MB);

        receiver.context().fileTransmit().addFileIoChannelHandler(topic, new TransmitSessionFactory() {
            @Override public TransmitSession create() {
                return new TransmitSessionAdapter() {
                    @Override public void begin(UUID nodeId, String sessionId) {
                        if (failFirstTime.compareAndSet(false, true))
                            throw new IgniteException(exTestMessage);
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

                    @Override public void onException(Throwable cause) {
                        assertEquals(exTestMessage, cause.getMessage());
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

    /** */
    @Test
    public void testReconnectInTheMiddleOfTransmission() throws Exception {
        final String chunkDownloadExMsg = "Test exception. Chunk processing error.";

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 5, ByteUnit.MB);
        final AtomicInteger readedChunks = new AtomicInteger();

        receiver.context().fileTransmit().chunkedStreamFactory(new ChunkedStreamFactory() {
            @Override public ChunkedInputStream createInputStream(
                ReadPolicy policy,
                TransmitSession session,
                int chunkSize
            ) throws IgniteCheckedException {
                assertEquals(policy, ReadPolicy.FILE);

                return new ChunkedFileStream(session.fileHandler(), chunkSize) {
                    @Override public void readChunk(TransmitInputChannel in) throws IOException {
                        // Read 4 chunks than throw an exception to emulate error processing.
                        if (readedChunks.incrementAndGet() == 4)
                            throw new IOException(chunkDownloadExMsg);

                        super.readChunk(in);

                        assertTrue(transferred.get() > 0);
                    }
                };
            }
        });

        receiver.context().fileTransmit().addFileIoChannelHandler(topic, new TransmitSessionFactory() {
            @Override public TransmitSession create() {
                return new TransmitSessionAdapter() {
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

                    @Override public void onException(Throwable cause) {
                        assertEquals(chunkDownloadExMsg, cause.getMessage());
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

    /** */
    @Test
    public void testDownloadFileWithSpeedLimit() throws Exception {
        final int fileSizeMb = 5;
        final int donwloadSpeedMbSec = 1;

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeMb, ByteUnit.MB);

        receiver.context().fileTransmit().downloadRate(donwloadSpeedMbSec, ByteUnit.MB);

        receiver.context().fileTransmit().addFileIoChannelHandler(topic, new TransmitSessionFactory() {
            @Override public TransmitSession create() {
                return new TransmitSessionAdapter() {
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

        long startTime = U.currentTimeMillis();

        try (FileWriter writer = sender.context()
            .fileTransmit()
            .fileWriter(receiver.localNode().id(), topic, PUBLIC_POOL)) {
            writer.write(fileToSend, 0, fileToSend.length(), new HashMap<>(), ReadPolicy.FILE);
        }

        long totalTime = U.currentTimeMillis() - startTime;
        int limitMs = (fileSizeMb / donwloadSpeedMbSec) * 1000;

        assertTrue("Download speed exceeded the limit [actual=" + totalTime + ", limit=" + limitMs + ']',
            totalTime <= limitMs);
    }

    /** */
    @Test
    public void testChuckHandler() throws Exception {
        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 10, ByteUnit.MB);

        receiver.context().fileTransmit().addFileIoChannelHandler(topic, new TransmitSessionFactory() {
            @Override public TransmitSession create() {
                return new TransmitSessionAdapter() {
                    /** {@inheritDoc} */
                    @Override public ChunkHandler chunkHandler() {
                        return new ChunkHandler() {
                            /** */
                            private File file;

                            /** */
                            private FileIO fileIo;

                            @Override public int begin(
                                String name,
                                long position,
                                long count,
                                Map<String, Serializable> params
                            ) throws IOException {
                                file = new File(tempStore, name + "_" + receiver.localNode().id());

                                fileIo = ioFactory.create(file);

                                fileIo.position(position);

                                return 4 * 1024; // Page size
                            }

                            @Override public boolean chunk(ByteBuffer buff) throws IOException {
                                assertTrue(buff.order() == ByteOrder.nativeOrder());
                                assertEquals(0, buff.position());

                                fileIo.writeFully(buff);

                                return true;
                            }

                            @Override public void end(Map<String, Serializable> params) {
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
            writer.write(fileToSend, 0, fileToSend.length(), new HashMap<>(), ReadPolicy.BUFF);
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
