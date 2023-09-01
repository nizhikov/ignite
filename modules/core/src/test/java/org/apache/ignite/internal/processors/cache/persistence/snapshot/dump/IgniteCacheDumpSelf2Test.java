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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.DMP_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.KEYS_CNT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.CreateDumpFutureTask.DUMP_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpEntrySerializer.HEADER_SZ;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class IgniteCacheDumpSelf2Test extends GridCommonAbstractTest {
    /** */
    private LogListener lsnr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (lsnr != null) {
            ListeningTestLogger testLog = new ListeningTestLogger(log);

            testLog.registerListener(lsnr);

            cfg.setGridLogger(testLog);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testDumpFailIfNoCaches() throws Exception {
        try (IgniteEx ign = startGrid(new IgniteConfiguration())) {
            ign.cluster().state(ClusterState.ACTIVE);

            assertThrows(
                null,
                () -> ign.snapshot().createDump("dump").get(),
                IgniteException.class,
                "Dump operation has been rejected. No cache group defined in cluster"
            );
        }
    }

    /** */
    @Test
    public void testUnreadyDumpCleared() throws Exception {
        IgniteEx ign = (IgniteEx)startGridsMultiThreaded(2);

        ign.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ign.createCache(DEFAULT_CACHE_NAME);

        IntStream.range(0, KEYS_CNT).forEach(i -> cache.put(i, i));

        ign.snapshot().createDump(DMP_NAME).get(getTestTimeout());

        stopAllGrids();

        File dumpDir =
            new File(U.resolveWorkDirectory(U.defaultWorkDirectory(), ign.configuration().getSnapshotPath(), false), DMP_NAME);

        Dump dump = new Dump(ign.context(), dumpDir);

        List<String> nodes = dump.nodesDirectories();

        assertNotNull(nodes);
        assertEquals(2, nodes.size());

        File nodeDumpDir = new File(dumpDir, DB_DEFAULT_FOLDER + File.separator + nodes.get(0));

        assertTrue(new File(nodeDumpDir, DUMP_LOCK).createNewFile());

        lsnr = LogListener.matches("Found locked dump dir. " +
            "This means, dump creation not finished prior to node fail. " +
            "Directory will be deleted: " + nodeDumpDir.getAbsolutePath()).build();

        startGridsMultiThreaded(2);

        assertFalse(nodeDumpDir.exists());
        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testDumpIteratorFaileOnWrongCrc() throws Exception {
        try (IgniteEx ign = startGrid(new IgniteConfiguration())) {
            ign.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> cache = ign.createCache(DEFAULT_CACHE_NAME);

            for (int key : partitionKeys(cache, 0, KEYS_CNT, 0))
                cache.put(key, key);

            ign.snapshot().createDump(DMP_NAME).get();

            File dumpDir =
                new File(U.resolveWorkDirectory(U.defaultWorkDirectory(), ign.configuration().getSnapshotPath(), false), DMP_NAME);

            Dump dump = new Dump(ign.context(), dumpDir);

            List<String> nodes = dump.nodesDirectories();

            assertNotNull(nodes);
            assertEquals(1, nodes.size());

            File cacheDumpDir = new File(
                dumpDir,
                DB_DEFAULT_FOLDER + File.separator + nodes.get(0) + File.separator + CACHE_DIR_PREFIX + DEFAULT_CACHE_NAME
            );

            assertTrue(cacheDumpDir.exists());

            Set<File> dumpFiles = new HashSet<>(Arrays.asList(cacheDumpDir.listFiles()));

            assertEquals(2, dumpFiles.size());

            String partDumpName = PART_FILE_PREFIX + 0 + DUMP_FILE_EXT;

            assertTrue(dumpFiles.stream().anyMatch(f -> f.getName().equals(CACHE_DATA_FILENAME)));
            assertTrue(dumpFiles.stream().anyMatch(f -> f.getName().equals(partDumpName)));

            try (FileChannel fc = FileChannel.open(Paths.get(cacheDumpDir.getAbsolutePath(), partDumpName), READ, WRITE)) {
                fc.position(HEADER_SZ); // Skip first entry header.

                int bufSz = 5;

                ByteBuffer buf = ByteBuffer.allocate(bufSz);

                assertEquals(bufSz, fc.read(buf));

                buf.position(0);

                // Increment first five bytes in dumped entry.
                for (int i = 0; i < bufSz; i++) {
                    byte b = buf.get();
                    b++;
                    buf.position(i);
                    buf.put(b);
                }

                fc.position(HEADER_SZ);

                buf.rewind();
                fc.write(buf);
            }

            assertThrows(
                null,
                () -> dump.iterator(nodes.get(0), CU.cacheId(DEFAULT_CACHE_NAME)),
                IgniteException.class,
                "Data corrupted"
            );
        }
    }
}
