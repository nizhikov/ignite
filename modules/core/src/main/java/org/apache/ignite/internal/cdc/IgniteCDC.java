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

package org.apache.ignite.internal.cdc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.CaptureDataChangeConfiguration;
import org.apache.ignite.cdc.CaptureDataChangeConsumer;
import org.apache.ignite.cdc.ChangeEvent;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CDC_PATH;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.NODE_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.UUID_STR_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * CDC(Capture Data Change) application.
 * Application run independently of Ignite node process and provide ability for the {@link CaptureDataChangeConsumer} to consume events({@link ChangeEvent}) from WAL segments.
 * User should responsible {@link CaptureDataChangeConsumer} implementation with custom consumption logic.
 *
 * Ignite node should be explicitly configured for using {@link IgniteCDC}.
 * <ol>
 *     <li>Set {@link DataStorageConfiguration#setCdcEnabled(boolean)} to true.</li>
 *     <li>Optional: Set {@link DataStorageConfiguration#setCdcPath(String)} to path to the directory to store WAL segments for CDC.</li>
 *     <li>Optional: Set {@link DataStorageConfiguration#setWalForceArchiveTimeout(long)} to configure timeout for force WAL rollover,
 *     so new events will be available for consumptions with the predicted time.</li>
 * </ol>
 *
 * When {@link DataStorageConfiguration#getCdcPath()} is true then Ignite node on each WAL segment rollover creates hard link
 * to archive WAL segment in {@link DataStorageConfiguration#getCdcPath()} directory.
 * {@link IgniteCDC} application takes segment file and consumes events from it. After successful consumption (see {@link CaptureDataChangeConsumer#onChange(Iterator)})
 * WAL segment will be deleted from directory.
 *
 * Several Ignite nodes can be started on the same host.
 * If your deployment done with custom consistent id then you should specify it via {@link IgniteConfiguration#setConsistentId(Serializable)} in provided {@link IgniteConfiguration}.
 * Otherwise you should specify auto generated consistent id and node index with the following options:
 * <ul>
 *     <li>{@link CaptureDataChangeConfiguration#setAutoGeneratedConsistentId(UUID)}.</li>
 *     <li>{@link CaptureDataChangeConfiguration#setNodeIndex(int)}.</li>
 * </ul>
 *
 * Application works as follows:
 * <ol>
 *     <li>Search node work directory based on provided {@link IgniteConfiguration} and system properties.</li>
 *     <li>Await for creation of CDC directory if it not exists.</li>
 *     <li>Acquire file lock to ensure exclusive consumption.</li>
 *     <li>Loads state of consumption if it exists.</li>
 *     <li>Infinetely wait for new available segment and process it.</li>
 * </ol>
 *
 * @see DataStorageConfiguration#setCdcEnabled(boolean)
 * @see DataStorageConfiguration#setCdcPath(String)
 * @see DataStorageConfiguration#setWalForceArchiveTimeout(long)
 * @see CommandLineStartup
 * @see CaptureDataChangeConsumer
 * @see DataStorageConfiguration#DFLT_CDC_PATH
 */
public class IgniteCDC implements Runnable {
    /** State dir. */
    public static final String STATE_DIR = "state";

    /** Ignite configuration. */
    private final IgniteConfiguration cfg;

    /** CDC configuration. */
    private final CaptureDataChangeConfiguration cdcCfg;

    /** WAL iterator factory. */
    private final IgniteWalIteratorFactory factory;

    /** Events consumer. */
    private final WALRecordsConsumer<?, ?> consumer;

    /** Last time when new segment found. */
    private AtomicLongMetric segmentTime;

    /** Last new segment index. */
    private AtomicLongMetric segmentIndex;

    /** Last commit time. */
    private AtomicLongMetric commitTime;

    /** Index of last commited state. */
    private AtomicLongMetric commitedIndex;

    /** Offset of last commited state. */
    private IntMetricImpl commitedOffset;

    /** Metric registry. */
    private GridMetricManager mmgr;

    /** Logger. */
    private final IgniteLogger log;

    /** CDC directory. */
    private Path cdcDir;

    /** Binary meta directory. */
    private File binaryMeta;

    /** Marshaller directory. */
    private File marshaller;

    /** CDC state. */
    private CDCConsumerState state;

    /** Save state to start from. */
    private WALPointer initState;

    /** Consistent ID. */
    private final String nodeDir;

    /** Previous segments. */
    private final List<Path> prevSegments = new ArrayList<>();

    /**
     * @param cfg Ignite configuration.
     * @param cdcCfg CDC configuration.
     */
    public IgniteCDC(IgniteConfiguration cfg, CaptureDataChangeConfiguration cdcCfg) {
        this.cfg = cfg;
        this.cdcCfg = cdcCfg;
        this.consumer = new WALRecordsConsumer<>(cdcCfg.getConsumer());

        log = logger(cfg, workDir(cfg));
        factory = new IgniteWalIteratorFactory(log);

        if (!CU.isPersistenceEnabled(cfg)) {
            log.error("Persistence disabled. IgniteCDC can't run!");

            throw new IllegalArgumentException("Persistence disabled. IgniteCDC can't run!");
        }

        nodeDir = consistentId(cfg);

        if (nodeDir == null) {
            log.warning("Can't determine nodeDir. It is recommended to set Consistent ID for production " +
                "clusters(use IgniteConfiguration.setConsistentId or CaptureDataChangeConfiguration.setConsistentId " +
                "and CaptureDataChangeConfiguration.setNodeIndex).");
        }
    }

    /** Runs CDC. */
    @Override public void run() {
        try {
            runX();
        }
        catch (Throwable e) {
            e.printStackTrace();

            throw new RuntimeException(e);
        }
    }

    /** Runs CDC application with possible exception. */
    public void runX() throws Exception {
        if (log.isInfoEnabled()) {
            log.info("Starting Ignite CDC Application.");
            log.info("Consumer    -\t" + consumer.toString());
        }

        cdcDir = findCDCDir(workDir(cfg));

        try (CDCFileLockHolder lock =
                new CDCFileLockHolder(cdcDir.toString(), () -> "cdc.lock", log)) {
            log.info("Trying to acquire file lock[lock=" + lock.lockPath() + ']');

            lock.tryLock(cdcCfg.getLockTimeout());

            init();

            if (log.isInfoEnabled()) {
                log.info("CDC dir     -\t" + cdcDir);
                log.info("Binary meta -\t" + binaryMeta);
                log.info("Marshaller  -\t" + marshaller);
                log.info("--------------------------------");
            }

            state = new CDCConsumerState(cdcDir.resolve(STATE_DIR), "cdc-state.bin");

            initState = state.load();

            if (initState != null) {
                if (log.isInfoEnabled())
                    log.info("Loaded initial state[state=" + initState + ']');

                commitedIndex.value(initState.index());
                commitedOffset.value(initState.fileOffset());
            }

            consumer.start(cfg, mmgr, log);

            try {
                Predicate<Path> walFilesOnly = p -> WAL_NAME_PATTERN.matcher(p.getFileName().toString()).matches();

                Comparator<Path> sortByNumber = Comparator.comparingLong(this::segmentIndex);

                long[] lastSgmnt = new long[] { -1 };

                waitFor(cdcDir, walFilesOnly, sortByNumber, segment -> {
                    try {
                        long nextSgmnt = segmentIndex(segment);

                        assert lastSgmnt[0] == -1 || nextSgmnt - lastSgmnt[0] == 1;

                        lastSgmnt[0] = nextSgmnt;

                        segmentIndex.value(lastSgmnt[0]);
                        segmentTime.value(System.currentTimeMillis());

                        readSegment(segment);

                        return true;
                    }
                    catch (IgniteCheckedException | IOException e) {
                        throw new IgniteException(e);
                    }
                }, cdcCfg.getSleepBeforeCheckNewSegmentsTimeout(), log);
            }
            finally {
                consumer.stop();

                if (log.isInfoEnabled())
                    log.info("Ignite CDC Application stoped.");
            }
        }
    }

    /** Searches required directories. */
    private void init() throws IOException, IgniteCheckedException {
        String workDir = workDir(cfg);
        String consIdDir = cdcDir.getName(cdcDir.getNameCount() - 1).toString();

        Files.createDirectories(cdcDir.resolve(STATE_DIR));

        binaryMeta = CacheObjectBinaryProcessorImpl.binaryWorkDir(workDir, consIdDir);

        marshaller = MarshallerContextImpl.mappingFileStoreWorkDir(workDir);

        if (log.isDebugEnabled()) {
            log.debug("Using BinaryMeta directory[dir=" + binaryMeta + ']');
            log.debug("Using Marshaller directory[dir=" + marshaller + ']');
        }

        GridKernalContext kctx = new StandaloneGridKernalContext(log, binaryMeta, marshaller);

        mmgr = kctx.metric();

        MetricRegistry mreg = mmgr.registry(metricName("cdc", "internals"));

        mreg.longMetric("StartTime", "Application start time").value(System.currentTimeMillis());

        segmentTime = mreg.longMetric("LastSegmentTime", "Time last WAL segment was detected");
        segmentIndex = mreg.longMetric("LastSegmentIndex", "Last index of WAL segment detected");
        commitTime = mreg.longMetric("LastCommitTime", "List time state written to the disk");
        commitedIndex = mreg.longMetric("LastCommitIndex", "Index of segment where placed last commited offset");
        commitedOffset = mreg.intMetric("LastCommitOffset", "Offset in last commited segment");
    }

    /** Reads all available records from segment. */
    private void readSegment(Path segment) throws IgniteCheckedException, IOException {
        log.info("Processing WAL segment[segment=" + segment + ']');

        IgniteWalIteratorFactory.IteratorParametersBuilder builder = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .log(log)
            .binaryMetadataFileStoreDir(binaryMeta)
            .marshallerMappingFileStoreDir(marshaller)
            .keepBinary(cdcCfg.isKeepBinary())
            .filesOrDirs(segment.toFile())
            .addFilter((type, ptr) -> type == DATA_RECORD_V2);

        if (initState != null) {
            long segmentIdx = segmentIndex(segment);

            if (segmentIdx > initState.index()) {
                log.error("Found segment greater then saved state. Some events are missed. Exiting!" +
                    "[state=" + initState + ",segment=" + segmentIdx + ']');

                throw new IgniteException("Some data missed.");
            }
            else if (segmentIdx < initState.index()) {
                if (log.isInfoEnabled()) {
                    log.info("Deleting segment. Saved state has greater index.[segment=" +
                        segmentIdx + ",state=" + initState.index() + ']');
                }

                // WAL segment is a hard link to a segment file in the special CDC folder.
                // So, we can safely delete it after processing.
                Files.delete(segment);
            }
            else {
                builder.from(initState);

                initState = null;
            }
        }

        try (WALIterator it = factory.iterator(builder)) {
            while (it.hasNext()) {
                boolean commit = consumer.onRecords(F.iterator(it.iterator(), IgniteBiTuple::get2, true));

                if (commit) {
                    assert it.lastRead().isPresent();

                    WALPointer ptr = it.lastRead().get();

                    state.save(ptr);

                    commitTime.value(System.currentTimeMillis());
                    commitedIndex.value(ptr.index());
                    commitedOffset.value(ptr.fileOffset());

                    // Can delete after new file state save.
                    if (!prevSegments.isEmpty()) {
                        // WAL segment is a hard link to a segment file in a specifal CDC folder.
                        // So we can safely delete it after success processing.
                        for (Path prevSegment : prevSegments)
                            Files.deleteIfExists(prevSegment);

                        prevSegments.clear();
                    }
                }
            }
        }

        prevSegments.add(segment);
    }

    /**
     * @param workDir Working directory.
     * @return Path to CDC directory.
     */
    private Path findCDCDir(String workDir) throws InterruptedException {
        Path parent;

        if (cfg.getDataStorageConfiguration() != null &&
            !F.isEmpty(cfg.getDataStorageConfiguration().getCdcPath())) {
            parent = Paths.get(cfg.getDataStorageConfiguration().getCdcPath());

            if (!parent.isAbsolute())
                parent = Paths.get(workDir, cfg.getDataStorageConfiguration().getCdcPath());
        }
        else
            parent = Paths.get(workDir).resolve(DFLT_CDC_PATH);

        log.info("CDC root[dir=" + parent + ']');

        final Path[] cdcDir = new Path[1];

        String nodePattern = nodeDir == null ? (NODE_PATTERN + UUID_STR_PATTERN) : nodeDir;

        log.info("ConsistendId pattern[dir=" + nodePattern + ']');

        waitFor(parent,
            dir -> dir.getName(dir.getNameCount() - 1).toString().matches(nodePattern),
            Path::compareTo,
            dir -> {
                cdcDir[0] = dir;

                return false;
            },
            cdcCfg.getSleepBeforeCheckNewSegmentsTimeout(),
            log
        );

        return cdcDir[0];
    }

    /**
     * Initialize logger.
     *
     * @param icfg Configuration.
     */
    private static IgniteLogger logger(IgniteConfiguration icfg, String workDir) {
        try {
            IgniteLogger log = IgnitionEx.IgniteNamedInstance.initLogger(icfg.getGridLogger(), null, "cdc", workDir);

            return log;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Resolves work directory.
     *
     * @return Working directory
     */
    public static String workDir(IgniteConfiguration icfg) {
        try {
            String igniteHome = icfg.getIgniteHome();

            // Set Ignite home.
            if (igniteHome == null)
                igniteHome = U.getIgniteHome();
            else
                // If user provided IGNITE_HOME - set it as a system property.
                U.setIgniteHome(igniteHome);

            String userProvidedWorkDir = icfg.getWorkDirectory();

            // Correctly resolve work directory and set it back to configuration.
            return U.workDirectory(userProvidedWorkDir, igniteHome);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param segment WAL segment file.
     * @return Segment index.
     */
    public long segmentIndex(Path segment) {
        String fn = segment.getFileName().toString();

        return Long.parseLong(fn.substring(0, fn.indexOf('.')));
    }

    /**
     * @param icfg Configuration.
     * @return Consistent id to use.
     */
    private String consistentId(IgniteConfiguration icfg) {
        if (icfg.getConsistentId() != null)
            return U.maskForFileName(icfg.getConsistentId().toString());

        if (cdcCfg.getAutoGeneratedConsistentId() == null) {
            log.warning("ConsitentId is null.");

            return null;
        }

        return PdsConsistentIdProcessor.genNewStyleSubfolderName(cdcCfg.getNodeIndex(), cdcCfg.getAutoGeneratedConsistentId());
    }

    /**
     * Waits for the files or directories to be created insied {@code watchDir}
     * and if new file pass the {@code filter} then {@code callback} notified with the newly create file.
     * {@code callback} will allso be notified about already existing files that passes the filter.
     *
     * @param watchDir Directory to watch.
     * @param filter Filter of events.
     * @param sorter Sorter of files.
     * @param callback Callback to be notified.
     */
    @SuppressWarnings("BusyWait")
    public static void waitFor(Path watchDir, Predicate<Path> filter, Comparator<Path> sorter,
        Predicate<Path> callback, long timeout, IgniteLogger log) throws InterruptedException {
        // If watch dir not exists waiting for it creation.
        if (!Files.exists(watchDir))
            waitFor(watchDir.getParent(), watchDir::equals, Path::compareTo, p -> false, timeout, log);

        try {
            // Clear deleted file.
            Set<Path> seen = new HashSet<>();

            while (true) {
                try (Stream<Path> children = Files.walk(watchDir, 1).filter(p -> !p.equals(watchDir))) {
                    final boolean[] status = {true};

                    children
                        .filter(filter.and(p -> !seen.contains(p)))
                        .sorted(sorter)
                        .peek(seen::add)
                        .peek(p -> {
                            if (log.isDebugEnabled())
                                log.debug("New file[evt=" + p.toAbsolutePath() + ']');

                            if (status[0])
                                status[0] = callback.test(p);
                        }).count();

                    if (!status[0])
                        return;
                }

                Thread.sleep(timeout);
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }
}
