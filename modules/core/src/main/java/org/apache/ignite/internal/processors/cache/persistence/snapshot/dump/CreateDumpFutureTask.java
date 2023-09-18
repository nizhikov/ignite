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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractCreateSnapshotFutureTask;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFutureTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotSender;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.cacheDataFilename;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_LOCK;

/**
 * Task creates cache group dump.
 * Dump is a consistent snapshot of cache entries.
 * Directories structure is same as a full snapshot but each partitions saved in "part-0.dump" file.
 * Files structure is a set of {@link DumpEntry} written one by one.
 *
 * @see Dump
 * @see DumpEntry
 */
public class CreateDumpFutureTask extends AbstractCreateSnapshotFutureTask implements DumpEntryChangeListener {
    /** Dump files name. */
    public static final String DUMP_FILE_EXT = ".dump";

    /** Root dump directory. */
    private final File dumpDir;

    /** */
    private final FileIOFactory ioFactory;

    /**
     * Dump contextes.
     * Key is [group_id, partition_id] combined in single long value.
     *
     * @see #toLong(int, int)
     */
    private final Map<Long, PartitionDumpContext> dumpCtxs = new ConcurrentHashMap<>();

    /** Local node is primary for set of group partitions. */
    private final Map<Integer, Set<Integer>> grpPrimaries = new ConcurrentHashMap<>();

    /**
     * @param cctx Cache context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param dumpName Dump name.
     * @param ioFactory IO factory.
     * @param snpSndr Snapshot sender.
     * @param parts Parts to dump.
     */
    public CreateDumpFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqId,
        String dumpName,
        File dumpDir,
        FileIOFactory ioFactory,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        super(
            cctx,
            srcNodeId,
            reqId,
            dumpName,
            snpSndr,
            parts
        );

        this.dumpDir = dumpDir;
        this.ioFactory = ioFactory;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        try {
            log.info("Start cache dump [name=" + snpName + ", grps=" + parts.keySet() + ']');

            createDumpLock();

            processPartitions();

            prepare();

            saveSnapshotData();
        }
        catch (IgniteCheckedException | IOException e) {
            acceptException(e);
        }

        return false; // Don't wait for checkpoint.
    }

    /** {@inheritDoc} */
    @Override protected void processPartitions() throws IgniteCheckedException {
        super.processPartitions();

        processed.values().forEach(parts -> parts.remove(INDEX_PARTITION));
    }

    /** Prepares all data structures to dump entries. */
    private void prepare() throws IOException, IgniteCheckedException {
        for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
            int grp = e.getKey();

            File grpDumpDir = groupDirectory(cctx.cache().cacheGroup(grp));

            if (!grpDumpDir.mkdirs())
                throw new IgniteCheckedException("Dump directory can't be created: " + grpDumpDir);

            CacheGroupContext gctx = cctx.cache().cacheGroup(grp);

            for (GridCacheContext<?, ?> cctx : gctx.caches())
                cctx.dumpListener(this);

            grpPrimaries.put(
                grp,
                gctx.affinity().primaryPartitions(gctx.shared().kernalContext().localNodeId(), gctx.affinity().lastVersion())
            );
        }
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveCacheConfigs() {
        return parts.keySet().stream().map(grp -> runAsync(() -> {
            CacheGroupContext gctx = cctx.cache().cacheGroup(grp);

            File grpDir = groupDirectory(gctx);

            IgniteUtils.ensureDirectory(grpDir, "dump group directory", null);

            for (GridCacheContext<?, ?> cacheCtx : gctx.caches()) {
                DynamicCacheDescriptor desc = cctx.kernalContext().cache().cacheDescriptor(cacheCtx.cacheId());

                StoredCacheData cacheData = new StoredCacheData(new CacheConfiguration(desc.cacheConfiguration()));

                cacheData.queryEntities(desc.schema().entities());
                cacheData.sql(desc.sql());

                cctx.cache().configManager().writeCacheData(cacheData, new File(grpDir, cacheDataFilename(cacheData.config())));
            }
        })).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveGroup(int grp, Set<Integer> grpParts) {
        long start = System.currentTimeMillis();

        AtomicLong entriesCnt = new AtomicLong();
        AtomicLong changedEntriesCnt = new AtomicLong();
        AtomicLong partsRemain = new AtomicLong(grpParts.size());

        String name = cctx.cache().cacheGroup(grp).cacheOrGroupName();

        CacheGroupContext gctx = cctx.kernalContext().cache().cacheGroup(grp);

        log.info("Start group dump [name=" + name + ", id=" + grp + ']');

        return grpParts.stream().map(part -> runAsync(() -> {
            long entriesCnt0 = 0;

            try (PartitionDumpContext dumpCtx = dumpContext(grp, part)) {
                try (GridCloseableIterator<CacheDataRow> rows = gctx.offheap().reservedIterator(part, dumpCtx.topVer)) {
                    if (rows == null)
                        throw new IgniteCheckedException("Partition missing [part=" + part + ']');

                    while (rows.hasNext()) {
                        CacheDataRow row = rows.next();

                        assert row.partition() == part;

                        int cache = row.cacheId() == 0 ? grp : row.cacheId();

                        if (dumpCtx.writeForIterator(cache, row.expireTime(), row.key(), row.value(), row.version()))
                            entriesCnt0++;
                    }
                }

                entriesCnt.addAndGet(entriesCnt0);
                changedEntriesCnt.addAndGet(dumpCtx.changedCnt.intValue());

                long remain = partsRemain.decrementAndGet();

                if (remain == 0) {
                    clearDumpListener(gctx);

                    log.info("Finish group dump [name=" + name +
                        ", id=" + grp +
                        ", time=" + (System.currentTimeMillis() - start) +
                        ", iteratorEntriesCount=" + entriesCnt +
                        ", changedEntriesCount=" + changedEntriesCnt + ']');
                }
                else if (log.isDebugEnabled()) {
                    log.debug("Finish group partition dump [name=" + name +
                        ", id=" + grp +
                        ", part=" + part +
                        ", time=" + (System.currentTimeMillis() - start) +
                        ", iteratorEntriesCount=" + entriesCnt +
                        ", changedEntriesCount=" + changedEntriesCnt + ']');

                }
            }
        })).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void beforeChange(GridCacheContext cctx, KeyCacheObject key, CacheObject val, long expireTime, GridCacheVersion ver) {
        try {
            assert key.partition() != -1;

            dumpContext(cctx.groupId(), key.partition()).writeChanged(cctx.cacheId(), expireTime, key, val, ver);
        }
        catch (IgniteException e) {
            acceptException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected CompletableFuture<Void> closeAsync() {
        if (closeFut == null) {
            dumpCtxs.values().forEach(PartitionDumpContext::close);

            Throwable err0 = err.get();

            Set<GroupPartitionId> taken = new HashSet<>();

            for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
                int grp = e.getKey();

                clearDumpListener(cctx.cache().cacheGroup(grp));

                for (Integer part : e.getValue())
                    taken.add(new GroupPartitionId(grp, part));
            }

            closeFut = CompletableFuture.runAsync(
                () -> onDone(new SnapshotFutureTaskResult(taken, null), err0),
                cctx.kernalContext().pools().getSystemExecutorService()
            );
        }

        return closeFut;
    }

    /** */
    private void clearDumpListener(CacheGroupContext gctx) {
        for (GridCacheContext<?, ?> cctx : gctx.caches())
            cctx.dumpListener(null);
    }

    /** */
    private void createDumpLock() throws IgniteCheckedException, IOException {
        File nodeDumpDir = IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx);

        if (!nodeDumpDir.mkdirs())
            throw new IgniteCheckedException("Can't create node dump directory: " + nodeDumpDir.getAbsolutePath());

        File lock = new File(nodeDumpDir, DUMP_LOCK);

        if (!lock.createNewFile())
            throw new IgniteCheckedException("Lock file can't be created or already exists: " + lock.getAbsolutePath());
    }

    /** */
    private PartitionDumpContext dumpContext(int grp, int part) {
        return dumpCtxs.computeIfAbsent(
            toLong(grp, part),
            key -> new PartitionDumpContext(cctx.kernalContext().cache().cacheGroup(grp), part)
        );
    }

    /**
     * Context of dump single partition.
     */
    private class PartitionDumpContext implements Closeable {
        /** Group id. */
        final int grp;

        /** Partition id. */
        final int part;

        /** Hashes of cache keys of entries changed by the user during partition dump. */
        final Map<Integer, Set<KeyCacheObject>> changed;

        /** Count of entries changed during dump creation. */
        LongAdder changedCnt = new LongAdder();

        /** Partition dump file. Lazily initialized to prevent creation files for empty partitions. */
        final FileIO file;

        /** Last version on time of dump start. Can be used only for primary. */
        @Nullable final GridCacheVersion startVer;

        /** Topology Version. */
        private final AffinityTopologyVersion topVer;

        /** Partition serializer. */
        DumpEntrySerializer serdes;

        /** If {@code true} context is closed. */
        volatile boolean closed;

        /** Count of writers. When count becomes {@code 0} context must be closed. */
        private final AtomicInteger writers = new AtomicInteger(1); // Iterator writing entries to this context, by default.

        /**
         * @param gctx Group context.
         * @param part Partition id.
         */
        public PartitionDumpContext(CacheGroupContext gctx, int part) {
            assert gctx != null;

            try {
                this.part = part;
                grp = gctx.groupId();
                topVer = gctx.topology().lastTopologyChangeVersion();

                startVer = grpPrimaries.get(gctx.groupId()).contains(part) ? gctx.shared().versions().last() : null;

                serdes = new DumpEntrySerializer();
                changed = new HashMap<>();

                for (int cache : gctx.cacheIds())
                    changed.put(cache, new GridConcurrentHashSet<>());

                File dumpFile = new File(groupDirectory(gctx), PART_FILE_PREFIX + part + DUMP_FILE_EXT);

                if (!dumpFile.createNewFile())
                    throw new IgniteException("Dump file can't be created: " + dumpFile);

                file = ioFactory.create(dumpFile);
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * Writes entry that changed while dump creation in progress.
         * @param cache Cache id.
         * @param expireTime Expire time.
         * @param key Key.
         * @param val Value before change.
         * @param ver Version before change.
         */
        public void writeChanged(
            int cache,
            long expireTime,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver
        ) {
            String reasonToSkip = null;

            if (closed) // Partition already saved in dump.
                reasonToSkip = "partition already saved";
            else {
                writers.getAndIncrement();

                try {
                    if (startVer != null && ver.isGreater(startVer))
                        reasonToSkip = "greater version";
                    else if (!changed.get(cache).add(key)) // Entry changed several time during dump.
                        reasonToSkip = "changed several times";
                    else if (val == null)
                        reasonToSkip = "newly created or already removed"; // Previous value is null. Entry created after dump start, skip.
                    else {
                        synchronized (this) {
                            if (closed) // Partition already saved in dump.
                                reasonToSkip = "partition already saved";
                            else
                                write(cache, expireTime, key, val);
                        }

                        changedCnt.increment();
                    }
                }
                finally {
                    writers.decrementAndGet();
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Listener [grp=" + grp +
                    ", cache=" + cache +
                    ", part=" + part +
                    ", key=" + key +
                    ", written=" + (reasonToSkip == null ? "true" : reasonToSkip) + ']');
            }
        }

        /**
         * Writes entry fetched from partition iterator.
         *
         * @param cache Cache id.
         * @param expireTime Expire time.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @return {@code True} if entry was written in dump,
         * {@code false} if it was already written by {@link #writeChanged(int, long, KeyCacheObject, CacheObject, GridCacheVersion)}.
         */
        public boolean writeForIterator(
            int cache,
            long expireTime,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver
        ) {
            boolean written = true;

            if (startVer != null && ver.isGreater(startVer))
                written = false;
            else if (changed.get(cache).contains(key))
                written = false;
            else {
                synchronized (this) {
                    write(cache, expireTime, key, val);
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Iterator [" +
                    "grp=" + grp +
                    ", cache=" + cache +
                    ", part=" + part +
                    ", key=" + key +
                    ", written=" + written + ']');
            }

            return written;
        }

        /** */
        private void write(int cache, long expireTime, KeyCacheObject key, CacheObject val) {
            assert !closed;

            try {
                ByteBuffer buf = serdes.writeToBuffer(cache, expireTime, key, val, cctx.cacheObjectContext(cache));

                if (file.writeFully(buf) != buf.limit())
                    throw new IgniteException("Can't write row");
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            if (closed)
                return;

            synchronized (this) {
                closed = true;
            }

            writers.decrementAndGet();

            while (writers.get() > 0)
                LockSupport.parkNanos(1_000_000);

            U.closeQuiet(file);

            serdes = null;
        }
    }

    /** */
    public static long toLong(int high, int low) {
        return (((long)high) << Integer.SIZE) | (low & 0xffffffffL);
    }

    /** */
    private File groupDirectory(CacheGroupContext grpCtx) throws IgniteCheckedException {
        return new File(
            IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx),
            (grpCtx.caches().size() > 1 ? CACHE_GRP_DIR_PREFIX : CACHE_DIR_PREFIX) + grpCtx.cacheOrGroupName()
        );
    }
}
