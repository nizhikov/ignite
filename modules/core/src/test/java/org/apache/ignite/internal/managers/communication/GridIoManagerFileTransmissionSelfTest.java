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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoManager.FileWriter;
import org.apache.ignite.internal.managers.communication.chunk.FileReceiver;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;

/**
 * Test file transmission mamanger operations.
 */
public class GridIoManagerFileTransmissionSelfTest extends GridCommonAbstractTest {
    /** Number of cache keys to generate. */
    private static final long CACHE_SIZE = 50_000L;

    /** Temporary directory to store files. */
    private static final String TEMP_FILES_DIR = "ctmp";

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
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME));
    }

    /**
     * Transmit all cache partition to particular topic on the remote node.
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

        Map<String, Long> fileSizes = new HashMap<>();
        Map<String, Integer> fileCrcs = new HashMap<>();
        Map<String, Serializable> fileParams = new HashMap<>();

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public void onBegin(UUID nodeId) {
                assertEquals(sender.localNode().id(), nodeId);
            }

            @Override public FileHandler fileHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) {
                return new FileHandler() {
                    /** */
                    private final AtomicBoolean inited = new AtomicBoolean();

                    @Override public String path() {
                        assertTrue(inited.compareAndSet(false, true));

                        return new File(tempStore, name).getAbsolutePath();
                    }

                    @Override public void accept(File file) {
                        assertTrue(fileSizes.containsKey(file.getName()));
                        // Save all params.
                        fileParams.putAll(params);
                    }
                };
            }
        });

        File cacheDirIg0 = cacheWorkDir(sender, DEFAULT_CACHE_NAME);

        File[] cacheParts = cacheDirIg0.listFiles(fileBinFilter);

        for (File file : cacheParts) {
            fileSizes.put(file.getName(), file.length());
            fileCrcs.put(file.getName(), FastCrc.calcCrc(file));
        }

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            // Iterate over cache partition cacheParts.
            for (File file : cacheParts) {
                writer.write(file,
                    // Put additional params <file_name, file_hashcode> to map.
                    new HashMap<String, Serializable>() {{
                        put(file.getName(), file.hashCode());
                    }},
                    ReadPolicy.FILE);
            }
        }

        log.info("Writing test cacheParts finished. All Ignite instances will be stopped.");

        stopAllGrids();

        assertEquals(fileSizes.size(), tempStore.listFiles(fileBinFilter).length);

        for (File file : cacheParts) {
            // Check received file lenghs
            assertEquals("Received file lenght is incorrect: " + file.getName(),
                fileSizes.get(file.getName()), new Long(file.length()));

            // Check received params
            assertEquals("File additional parameters are not fully transmitted",
                fileParams.get(file.getName()), file.hashCode());
        }

        // Check received file CRCs.
        for (File file : tempStore.listFiles(fileBinFilter)) {
            assertEquals("Received file CRC-32 checksum is incorrect: " + file.getName(),
                fileCrcs.get(file.getName()), new Integer(FastCrc.calcCrc(file)));
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerOnBeginFails() throws Exception {
        final String exTestMessage = "Test exception. Handler initialization failed at onBegin.";

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("1Mb", 1024 * 1024);

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public void onBegin(UUID nodeId) {
                throw new IgniteException(exTestMessage);
            }

            @Override public FileHandler fileHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) {
                return getDefaultFileHandler(receiver, fileToSend, name);
            }

            @Override public void onException(UUID nodeId, Throwable err) {
                assertEquals(exTestMessage, err.getMessage());
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerOnReceiverNodeLeft() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicInteger chunksCnt = new AtomicInteger();

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);

        receiver.context().io()
            .chunkReceiverFactory((nodeId, hndlr, meta, checker) ->
                new FileReceiver(
                    meta.name(),
                    meta.offset(),
                    meta.count(),
                    meta.params(),
                    checker,
                    hndlr.fileHandler(nodeId,
                        meta.name(),
                        meta.offset(),
                        meta.count(),
                        meta.params())) {
                    @Override public void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException {
                        // Read 5 chunks than stop the grid.
                        if (chunksCnt.incrementAndGet() == 5)
                            stopGrid(receiver.name(), true);

                        super.readChunk(ch);
                    }
                });

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public FileHandler fileHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) {
                return getDefaultFileHandler(receiver, fileToSend, name);
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerReconnectOnReadFail() throws Exception {
        final String chunkDownloadExMsg = "Test exception. Chunk processing error.";

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 5 * 1024 * 1024);
        final AtomicInteger readedChunks = new AtomicInteger();

        receiver.context().io()
            .chunkReceiverFactory((nodeId, hndlr, meta, checker) -> {
                assertEquals(meta.policy(), ReadPolicy.FILE);

                return new FileReceiver(
                    meta.name(),
                    meta.offset(),
                    meta.count(),
                    meta.params(),
                    checker,
                    hndlr.fileHandler(nodeId,
                        meta.name(),
                        meta.offset(),
                        meta.count(),
                        meta.params())) {
                    @Override public void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException {
                        // Read 4 chunks than throw an exception to emulate error processing.
                        if (readedChunks.incrementAndGet() == 4)
                            throw new IgniteException(chunkDownloadExMsg);

                        super.readChunk(ch);

                        assertTrue(transferred > 0);
                    }
                };
            });

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public FileHandler fileHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) {
                return getDefaultFileHandler(receiver, fileToSend, name);
            }

            @Override public void onException(UUID nodeId, Throwable err) {
                assertEquals(chunkDownloadExMsg, err.getMessage());
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerReconnectOnInitFail() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicBoolean throwFirstTime = new AtomicBoolean();

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public FileHandler fileHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) {
                return new FileHandler() {
                    @Override public String path() {
                        if (throwFirstTime.compareAndSet(false, true))
                            throw new IgniteException("Test exception. Initialization fail.");

                        return new File(tempStore, name + "_" + receiver.localNode().id())
                            .getAbsolutePath();
                    }

                    @Override public void accept(File file) {
                        assertEquals(fileToSend.length(), file.length());
                        assertCrcEquals(fileToSend, file);
                    }
                };
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerNextWriterOpened() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicBoolean networkExThrown = new AtomicBoolean();

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("File_5MB", fileSizeBytes);

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public void onException(UUID nodeId, Throwable err) {
                assertEquals("Previous session is not closed properly", IgniteCheckedException.class, err.getClass());
                assertTrue(err.getMessage().startsWith("The handler has been aborted"));
            }

            @Override public FileHandler fileHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) {
                return new FileHandler() {
                    @Override public String path() {
                        if (networkExThrown.compareAndSet(false, true))
                            return null;

                        return new File(tempStore, name + "_" + receiver.localNode().id())
                            .getAbsolutePath();
                    }

                    @Override public void accept(File file) {
                        assertEquals(fileToSend.length(), file.length());
                        assertCrcEquals(fileToSend, file);
                    }
                };
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            // Expected exception.
            assertTrue(e.toString(), e.getCause().getMessage().startsWith("Channel processing error"));
        }

        //Open next session and complete successfull.
        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.FILE);
        }

        // Remove topic handler and fail
        receiver.context().io().removeTransmissionHandler(topic);

        IgniteCheckedException err = null;

        // Open next writer on removed topic.
        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            // Must catch execption here.
            err = e;
        }

        assertNotNull(err);
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerChannelCloseIfAnotherOpened() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final CountDownLatch waitLatch = new CountDownLatch(2);
        final CountDownLatch completionWait = new CountDownLatch(2);

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("file5MBSize", fileSizeBytes);

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            @Override public void onBegin(UUID nodeId) {
                waitLatch.countDown();

                try {
                    waitLatch.await(5, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }

            @Override public FileHandler fileHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) {
                return getDefaultFileHandler(receiver, fileToSend, name);
            }
        });

        IgniteCheckedException[] errs = new IgniteCheckedException[1];

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic);
             FileWriter anotherWriter = sender.context()
                 .io()
                 .openFileWriter(receiver.localNode().id(), topic)) {
            // Will connect on write attempt.
            GridTestUtils.runAsync(() -> {
                try {
                    writer.write(fileToSend, ReadPolicy.FILE);
                }
                catch (IgniteCheckedException e) {
                    errs[0] = e;
                }
                finally {
                    completionWait.countDown();
                }
            });

            GridTestUtils.runAsync(() -> {
                try {
                    anotherWriter.write(fileToSend, ReadPolicy.FILE);
                }
                catch (IgniteCheckedException e) {
                    errs[0] = e;
                }
                finally {
                    completionWait.countDown();
                }
            });

            waitLatch.await(5, TimeUnit.SECONDS);

            // Expected that one of the writers will throw exception.
            assertFalse("An error must be thrown if connected to the same topic during processing",
                errs[0] == null);

            completionWait.await(5, TimeUnit.SECONDS);

            throw errs[0];
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testChunkHandlerBase() throws Exception {
        final FileIO[] fileIo = new FileIO[1];

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 10 * 1024 * 1024);

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            /** {@inheritDoc} */
            @Override public ChunkHandler chunkHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) throws IgniteCheckedException {

                File file = new File(tempStore, name + "_" + receiver.localNode().id());

                try {
                    fileIo[0] = new RandomAccessFileIOFactory().create(file);
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }

                return new ChunkHandler() {
                    @Override public int size() {
                        return 1024; // Page size
                    }

                    @Override public void accept(ByteBuffer buff) throws IgniteCheckedException {
                        try {
                            assertTrue(buff.order() == ByteOrder.nativeOrder());
                            assertEquals(0, buff.position());
                            assertEquals(buff.limit(), buff.capacity());

                            fileIo[0].writeFully(buff);
                        }
                        catch (IOException e) {
                            throw new IgniteCheckedException(e);
                        }
                    }

                    @Override public void close() throws IOException {
                        assertEquals(fileToSend.length(), file.length());
                        assertCrcEquals(fileToSend, file);
                        U.closeQuiet(fileIo[0]);
                    }
                };
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.BUFF);
        }
        finally {
            U.closeQuiet(fileIo[0]);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testChunkHandlerReconnectOnInitFail() throws Exception {
        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 1024 * 1024);

        receiver.context().io().addTransmissionHandler(topic, new TransmissionHandlerAdapter() {
            /** {@inheritDoc} */
            @Override public ChunkHandler chunkHandler(UUID nodeId, String name, long offset, long cnt,
                Map<String, Serializable> params) {
                return new ChunkHandler() {
                    @Override public int size() {
                        throw new IgniteException("Test exception. Initialization failed");
                    }

                    @Override public void accept(ByteBuffer buff) throws IgniteCheckedException {
                        // No-op.
                    }

                    @Override public void close() throws IOException {
                        // No-op.
                    }
                };
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, ReadPolicy.BUFF);
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param cacheName Cache name to add data to.
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
     * @param receiver The node instance.
     * @param fileToSend The file to send.
     * @return Default file handler.
     */
    private FileHandler getDefaultFileHandler(IgniteEx receiver, File fileToSend, String name) {
        return new FileHandler() {
            @Override public String path() {
                return new File(tempStore, name + "_" + receiver.localNode().id()).getAbsolutePath();
            }

            @Override public void accept(File file) {
                assertEquals(fileToSend.length(), file.length());
                assertCrcEquals(fileToSend, file);
            }
        };
    }

    /**
     * @param fileToSend Source file to check CRC.
     * @param fileReceived Destination file to check CRC.
     */
    private static void assertCrcEquals(File fileToSend, File fileReceived) {
        try {
            assertEquals(FastCrc.calcCrc(fileToSend), FastCrc.calcCrc(fileReceived));
        }
        catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The defailt implementation of transmit session.
     */
    private static class TransmissionHandlerAdapter implements TransmissionHandler {
        /** {@inheritDoc} */
        @Override public void onBegin(UUID nodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public ChunkHandler chunkHandler(UUID nodeId, String name, long offset, long cnt,
            Map<String, Serializable> params) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FileHandler fileHandler(UUID nodeId, String name, long offset, long cnt,
            Map<String, Serializable> params) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onEnd(UUID nodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onException(UUID nodeId, Throwable err) {
            // No-op.
        }
    }
}
