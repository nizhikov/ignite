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

package org.apache.ignite.internal.managers.communication.transmit;

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
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.transmit.channel.InputTransmitChannel;
import org.apache.ignite.internal.managers.communication.transmit.chunk.ChunkedFile;
import org.apache.ignite.internal.managers.communication.transmit.chunk.ChunkedObjectFactory;
import org.apache.ignite.internal.managers.communication.transmit.chunk.ReadableChunkedObject;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
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
public class GridFileIoManagerSelfTest extends GridCommonAbstractTest {
    /** Number of cache keys to generate. */
    private static final long CACHE_SIZE = 50_000L;

    /** Temporary directory to store files. */
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

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public void onBegin(UUID nodeId) {
                assertEquals(sender.localNode().id(), nodeId);
            }

            @Override public FileHandler fileHandler(UUID nodeId) {
                return new FileHandler() {
                    /** */
                    private final AtomicBoolean inited = new AtomicBoolean();

                    @Override public String path(String name, Map<String, Serializable> params) {
                        assertTrue(inited.compareAndSet(false, true));

                        return new File(tempStore, name).getAbsolutePath();
                    }

                    @Override public void acceptFile(File file, long offset, long cnt, Map<String, Serializable> params) {
                        assertTrue(fileSizes.containsKey(file.getName()));
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
            for (File file : cacheParts)
                writer.write(file, new HashMap<>(), ReadPolicy.FILE);
        }

        log.info("Writing test cacheParts finished. All Ignite instances will be stopped.");

        stopAllGrids();

        assertEquals(fileSizes.size(), tempStore.listFiles(fileBinFilter).length);

        // Check received file lenghs
        for (File file : cacheParts)
            assertEquals("Received file lenght is incorrect: " + file.getName(),
                fileSizes.get(file.getName()), new Long(file.length()));

        // Check received file CRCs.
        for (File file : tempStore.listFiles(fileBinFilter))
            assertEquals("Received file CRC-32 checksum is incorrect: " + file.getName(),
                fileCrcs.get(file.getName()), new Integer(FastCrc.calcCrc(file)));
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

        File fileToSend = createFileRandomData("50Mb", 50 * 1024 * 1024);

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public void onBegin(UUID nodeId) {
                throw new IgniteException(exTestMessage);
            }

            @Override public FileHandler fileHandler(UUID nodeId) {
                return getDefaultFileHandler(receiver, fileToSend);
            }

            @Override public void onException(Throwable err) {
                assertEquals(exTestMessage, err.getMessage());
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = ClusterTopologyCheckedException.class)
    public void testFileHandlerOnReceiverNodeLeft() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicInteger chunksCnt = new AtomicInteger();

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);

        receiver.context().io().fileIoMgr().chunkedStreamFactory(new ChunkedObjectFactory() {
            @Override public ReadableChunkedObject createInputStream(
                UUID nodeId,
                ReadPolicy policy,
                FileTransmitHandler ses,
                int chunkSize
            ) throws IgniteCheckedException {
                return new ChunkedFile(ses.fileHandler(nodeId), chunkSize) {
                    @Override public void readChunk(InputTransmitChannel in) throws IOException {
                        // Read 5 chunks than stop the grid.
                        if (chunksCnt.incrementAndGet() == 5)
                            stopGrid(1, true);

                        super.readChunk(in);
                    }
                };
            }
        });

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public FileHandler fileHandler(UUID nodeId) {
                return getDefaultFileHandler(receiver, fileToSend);
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            if (e.hasCause(ClusterTopologyCheckedException.class))
                throw (ClusterTopologyCheckedException)e.getCause();
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = Exception.class)
    public void testFileHandlerReconnectOnReadFail() throws Exception {
        final String chunkDownloadExMsg = "Test exception. Chunk processing error.";

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 5 * 1024 * 1024);
        final AtomicInteger readedChunks = new AtomicInteger();

        receiver.context().io().fileIoMgr().chunkedStreamFactory(new ChunkedObjectFactory() {
            @Override public ReadableChunkedObject createInputStream(
                UUID nodeId,
                ReadPolicy policy,
                FileTransmitHandler ses,
                int chunkSize
            ) throws IgniteCheckedException {
                assertEquals(policy, ReadPolicy.FILE);

                return new ChunkedFile(ses.fileHandler(nodeId), chunkSize) {
                    @Override public void readChunk(InputTransmitChannel in) throws IOException {
                        // Read 4 chunks than throw an exception to emulate error processing.
                        if (readedChunks.incrementAndGet() == 4)
                            throw new IOException(chunkDownloadExMsg);

                        super.readChunk(in);

                        assertTrue(transferred.get() > 0);
                    }
                };
            }
        });

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public FileHandler fileHandler(UUID nodeId) {
                return getDefaultFileHandler(receiver, fileToSend);
            }

            @Override public void onException(Throwable err) {
                assertEquals(chunkDownloadExMsg, err.getMessage());
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            if (e.hasCause(Exception.class))
                throw (Exception)e.getCause();
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = Exception.class)
    public void testFileHandlerReconnectOnInitFail() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final AtomicBoolean throwFirstTime = new AtomicBoolean();

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public FileHandler fileHandler(UUID nodeId) {
                return new FileHandler() {
                    @Override public String path(String name, Map<String, Serializable> params) throws IOException {
                        if (throwFirstTime.compareAndSet(false, true))
                            throw new IOException("Test exception. Initialization fail.");

                        return new File(tempStore, name + "_" + receiver.localNode().id())
                            .getAbsolutePath();
                    }

                    @Override public void acceptFile(File file, long offset, long cnt, Map<String, Serializable> params) {
                        assertEquals(fileToSend.length(), file.length());
                        assertCrcEquals(fileToSend, file);
                    }
                };
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
        }
        catch (IgniteCheckedException e) {
            if (e.hasCause(Exception.class))
                throw (Exception)e.getCause();
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

        File fileToSend = createFileRandomData("File5MB", fileSizeBytes);

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public void onException(Throwable err) {
                assertEquals("Previous session is not closed properly", IgniteCheckedException.class, err.getClass());
                assertTrue(err.getMessage().startsWith("The handler has been aborted"));
            }

            @Override public FileHandler fileHandler(UUID nodeId) {
                return new FileHandler() {
                    @Override public String path(String name, Map<String, Serializable> params) throws IOException {
                        if (networkExThrown.compareAndSet(false, true))
                            return null;

                        return new File(tempStore, name + "_" + receiver.localNode().id())
                            .getAbsolutePath();
                    }

                    @Override public void acceptFile(File file, long offset, long cnt, Map<String, Serializable> params) {
                        assertEquals(fileToSend.length(), file.length());
                        assertCrcEquals(fileToSend, file);
                    }
                };
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
        }
        catch (Exception e) {
            // Expected exception.
            assertTrue(e.toString(), e.getCause().getMessage().startsWith("Error channel processing"));
        }

        //Open next session and complete successfull.
        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerWithDownloadLimit() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final int donwloadSpeedBytes = 1024 * 1024;

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);

        receiver.context().io().fileIoMgr().downloadRate(donwloadSpeedBytes);

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public FileHandler fileHandler(UUID nodeId) {
                return getDefaultFileHandler(receiver, fileToSend);
            }
        });

        long startTime = U.currentTimeMillis();

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
        }

        long totalTime = U.currentTimeMillis() - startTime;
        int limitMs = (fileSizeBytes / donwloadSpeedBytes) * 1000;

        log.info("Register the file download time  [total=" + totalTime + ", limit=" + limitMs + ']');

        assertTrue("Download speed exceeded the limit [total=" + totalTime + ", limit=" + limitMs + ']',
            totalTime <= limitMs);
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testFileHandlerChannelCloseIfAnotherOpened() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final CountDownLatch waitLatch = new CountDownLatch(2);

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("file5MBSize", fileSizeBytes);

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public void onBegin(UUID nodeId) {
                waitLatch.countDown();

                try {
                    waitLatch.await(5, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }

            @Override public FileHandler fileHandler(UUID nodeId) {
                return getDefaultFileHandler(receiver, fileToSend);
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
            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
                    }
                    catch (IgniteCheckedException e) {
                        errs[0] = e;
                    }
                }
            });
            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        anotherWriter.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
                    }
                    catch (IgniteCheckedException e) {
                        errs[0] = e;
                    }
                }
            });

            waitLatch.await(5, TimeUnit.SECONDS);

            // Expected that one of the writers will throw exception.
            if (errs[0] != null)
                throw errs[0];
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testFileHandlerWithUploadLimit() throws Exception {
        final int fileSizeBytes = 5 * 1024 * 1024;
        final int uploadSpeedBytesSec = 1024 * 1024;

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", fileSizeBytes);

        sender.context().io().fileIoMgr().uploadRate(uploadSpeedBytesSec);

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            @Override public FileHandler fileHandler(UUID nodeId) {
                return getDefaultFileHandler(receiver, fileToSend);
            }
        });

        long startTime = U.currentTimeMillis();

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.FILE);
        }

        long totalTime = U.currentTimeMillis() - startTime;
        int limitMs = (fileSizeBytes / uploadSpeedBytesSec) * 1000;

        log.info("Register the file upload time  [total=" + totalTime + ", limit=" + limitMs + ']');

        assertTrue("Upload speed exceeded the limit [total=" + totalTime + ", limit=" + limitMs + ']',
            totalTime <= limitMs);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testChuckHandlerBase() throws Exception {
        final FileIO[] fileIo = new FileIO[1];

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 10 * 1024 * 1024);

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            /** {@inheritDoc} */
            @Override public ChunkHandler chunkHandler(UUID nodeId) {
                return new ChunkHandler() {
                    /** */
                    private File file;

                    @Override public int begin(String name, Map<String, Serializable> params) throws IOException {
                        file = new File(tempStore, name + "_" + receiver.localNode().id());

                        fileIo[0] = ioFactory.create(file);

                        return 4 * 1024; // Page size
                    }

                    @Override public boolean chunk(ByteBuffer buff, long pos) throws IOException {
                        assertTrue(buff.order() == ByteOrder.nativeOrder());
                        assertEquals(0, buff.position());

                        fileIo[0].position(pos);
                        fileIo[0].writeFully(buff);

                        return true;
                    }

                    @Override public void end(Map<String, Serializable> params) {
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
            writer.write(fileToSend, new HashMap<>(), ReadPolicy.BUFF);
        }
        finally {
            U.closeQuiet(fileIo[0]);
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = Exception.class)
    public void testChuckHandlerReconnectOnInitFail() throws Exception {
        final FileIO[] fileIo = new FileIO[1];
        final AtomicBoolean throwFirstTime = new AtomicBoolean();

        IgniteEx sender = startGrid(0);
        IgniteEx receiver = startGrid(1);

        sender.cluster().active(true);

        File fileToSend = createFileRandomData("testFile", 10 * 1024 * 1024);

        receiver.context().io().addFileTransmitHandler(topic, new FileTransmitHandlerAdapter() {
            /** {@inheritDoc} */
            @Override public ChunkHandler chunkHandler(UUID nodeId) {
                return new ChunkHandler() {
                    /** */
                    private File file;

                    @Override public int begin(String name, Map<String, Serializable> params) throws IOException {
                        if (throwFirstTime.compareAndSet(false, true))
                            throw new IOException("Test exception. Initialization failed");

                        file = new File(tempStore, name + "_" + receiver.localNode().id());
                        fileIo[0] = ioFactory.create(file);

                        return 16 * 1024;
                    }

                    @Override public boolean chunk(ByteBuffer buff, long pos) throws IOException {
                        fileIo[0].writeFully(buff);

                        return true;
                    }

                    @Override public void end(Map<String, Serializable> params) {
                        assertEquals(fileToSend.length(), file.length());
                        assertCrcEquals(fileToSend, file);
                    }
                };
            }
        });

        try (FileWriter writer = sender.context()
            .io()
            .openFileWriter(receiver.localNode().id(), topic)) {
            writer.write(fileToSend, 0, fileToSend.length(), new HashMap<>(), ReadPolicy.BUFF);
        }
        catch (IgniteCheckedException e) {
            if (e.hasCause(Exception.class))
                throw (Exception)e.getCause();
        }
        finally {
            U.closeQuiet(fileIo[0]);
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
    private FileHandler getDefaultFileHandler(IgniteEx receiver, File fileToSend) {
        return new FileHandler() {
            @Override public String path(String name, Map<String, Serializable> params) {
                return new File(tempStore, name + "_" + receiver.localNode().id()).getAbsolutePath();
            }

            @Override public void acceptFile(File file, long offset, long cnt, Map<String, Serializable> params) {
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
    private static class FileTransmitHandlerAdapter implements FileTransmitHandler {
        /** {@inheritDoc} */
        @Override public void onBegin(UUID nodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public ChunkHandler chunkHandler(UUID nodeId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FileHandler fileHandler(UUID nodeId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onEnd(UUID nodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onException(Throwable err) {
            // No-op.
        }
    }
}
