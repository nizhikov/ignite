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
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.transmit.channel.InputTransmitChannel;
import org.apache.ignite.internal.managers.communication.transmit.channel.OutputTransmitChannel;
import org.apache.ignite.internal.managers.communication.transmit.channel.RemoteHandlerException;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitException;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitMeta;
import org.apache.ignite.internal.managers.communication.transmit.chunk.InputChunkedFile;
import org.apache.ignite.internal.managers.communication.transmit.chunk.ChunkedObjectFactory;
import org.apache.ignite.internal.managers.communication.transmit.chunk.InputChunkedObject;
import org.apache.ignite.internal.managers.communication.transmit.chunk.OutputChunkedFile;
import org.apache.ignite.internal.managers.communication.transmit.util.TimedSemaphore;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteTriClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Class represents a file transfer manager implementation to send or receive files between grid nodes.
 */
public class GridFileIoManager {
    /**
     * The default transfer chunk size in bytes. Setting the transfer chunk size
     * more than <tt>1 MB</tt> is meaningless because there is no asymptotic benefit.
     * What we're trying to achieve with larger transfer chunk sizes is fewer context
     * switches, and every time we double the transfer size you halve the context switch cost.
     * <p>
     * Default value is {@code 256Kb}.
     */
    public static final int DFLT_CHUNK_SIZE_BYTES = 256 * 1024;

    /** Retry attempts count to send single file if connection dropped (value is {@code 5}). */
    public static final int DFLT_RETRY_CNT = 5;

    /** The default file limit transmittion rate per node instance (value is {@code 500 MB/sec}). */
    public static final long DFLT_RATE_LIMIT_BYTES = 500 * 1024 * 1024;

    /** The default timeout for waiting the permits. */
    private static final int DFLT_ACQUIRE_TIMEOUT_MS = 5000;

    /** Flag send to remote on file writer close. */
    private static final int FILE_WRITER_CLOSED = -1;

    /** Ignite kernal context. */
    private final GridKernalContext ctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Map of registered handlers per each IO topic. */
    private final ConcurrentMap<Object, FileTransmitHandler> topicHandlerMap = new ConcurrentHashMap<>();

    /** The map of already known channel read contexts by its registered topics. */
    private final ConcurrentMap<Object, FileIoReadContext> sesCtxMap = new ConcurrentHashMap<>();

    /** Managers busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /**
     * The total amount of permits for the 1 second period of time per the node instance for download.
     * To limit the download speed of reading the stream of data we will acuire a permit per byte.
     * <p>
     * For instance, for the 128 Kb/sec rate you should specify total <tt>131_072</tt> permits.
     */
    private final TimedSemaphore inBytePermits;

    /**
     * The total amount of permits for the 1 second period of time per the node instance for upload.
     * See for details descriptoin of {@link #inBytePermits}.
     */
    private final TimedSemaphore outBytePermits;

    /** Closure to open a new channel to remote node. */
    private final IgniteTriClosure<UUID, Object, Message, IgniteInternalFuture<Channel>> openClsr;

    /** Listener to handle NODE_LEFT, NODE_FAIL events while waiting for remote reconnects. */
    private DiscoveryEventListener discoLsnr;

    /** Processor stopping flag. */
    private volatile boolean stopped;

    /** The factory produces chunked stream to process an input data channel. */
    private volatile ChunkedObjectFactory streamFactory = new ChunkedObjectFactory();

    /** The maximum number of retry attempts (read or write attempts). */
    private volatile int retryCnt = DFLT_RETRY_CNT;

    /** The size of stream chunks. */
    private int ioChunkSize = DFLT_CHUNK_SIZE_BYTES;

    /**
     * @param ctx Kernal context.
     */
    public GridFileIoManager(
        GridKernalContext ctx,
        IgniteTriClosure<UUID, Object, Message, IgniteInternalFuture<Channel>> openClsr
    ) {
        this.ctx = ctx;
        log = ctx.log(getClass());
        this.openClsr = openClsr;

        inBytePermits = new TimedSemaphore(DFLT_RATE_LIMIT_BYTES);
        outBytePermits = new TimedSemaphore(DFLT_RATE_LIMIT_BYTES);
    }

    /**
     * Get the maximum number of retry attempts used for uploading or downloading file when the socket connection
     * has been dropped by network issues.
     *
     * @return The number of retry attempts.
     */
    public int retryCnt() {
        return retryCnt;
    }

    /**
     * Set the maximum number of retry attempts used for uploading or downloading file when the socket connection
     * has been dropped by network issues.
     *
     * @param retryCnt The number of retry attempts.
     */
    public void retryCnt(int retryCnt) {
        this.retryCnt = retryCnt;
    }

    /**
     * Set the download rate per second in bytes. It is possible to modify the download rate at runtime.
     * Reducing the speed takes effect immediately by blocking incoming requests on the
     * semaphore {@link #inBytePermits}. If the speed is increased than waiting threads
     * are not released immediately, but will be wake up when the next time period of
     * {@link TimedSemaphore} runs out.
     * <p>
     * Setting the count to {@link TimedSemaphore#UNLIMITED_PERMITS} will switch off the
     * configured {@link #inBytePermits} limit.
     *
     * @param count Number of bytes per second for the donwload speed.
     */
    public void downloadRate(int count) {
        if (count <= 0)
            inBytePermits.permitsPerSec(TimedSemaphore.UNLIMITED_PERMITS);
        else
            inBytePermits.permitsPerSec(count);

        U.log(log, "The file download speed has been set to: " + count + " bytes per sec.");
    }

    /**
     * Set the upload rate per second in bytes. It is possible to modify the download rate at runtime.
     * Reducing the speed takes effect immediately by blocking incoming requests on the
     * semaphore {@link #outBytePermits}. If the speed is increased than waiting threads
     * are not released immediately, but will be wake up when the next time period of
     * {@link TimedSemaphore} runs out.
     * <p>
     * Setting the count to {@link TimedSemaphore#UNLIMITED_PERMITS} will switch off the configured
     * {@link #outBytePermits} limit.
     *
     * @param count Number of bytes per second for the upload speed.
     */
    public void uploadRate(int count) {
        if (count <= 0)
            outBytePermits.permitsPerSec(TimedSemaphore.UNLIMITED_PERMITS);
        else
            outBytePermits.permitsPerSec(count);

        U.log(log, "The file upload speed has been set to: " + count + " bytes per sec.");
    }

    /**
     * Callback that notifies that kernal has successfully started,
     * including all managers and processors.
     *
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void onKernalStart() throws IgniteCheckedException {
        ctx.event().addDiscoveryEventListener(discoLsnr = (evt, disco) -> {
            if (!busyLock.enterBusy())
                return;

            try {
                UUID leftNodeId = evt.eventNode().id();

                // Clear the context on the uploader node left.
                for (Map.Entry<Object, FileIoReadContext> sesEntry : sesCtxMap.entrySet()) {
                    FileIoReadContext ioctx = sesEntry.getValue();

                    if (ioctx.nodeId.equals(leftNodeId)) {
                        ClusterTopologyCheckedException ex;

                        ioctx.session.onException(ex = new ClusterTopologyCheckedException("Failed to proceed download. " +
                            "The remote node node left the grid: " + leftNodeId));

                        sesCtxMap.remove(sesEntry.getKey());
                    }
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * Callback to notify that kernal is about to stop.
     */
    public void onKernalStop() {
        busyLock.block();

        try {
            stopped = true;

            ctx.event().removeDiscoveryEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

            for (Object topic : topicHandlerMap.keySet())
                removeTransmitSessionHandler(topic);

            inBytePermits.shutdown();
            outBytePermits.shutdown();
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * @throws NodeStoppingException If node stopping.
     */
    private void checkNotStopped() throws NodeStoppingException {
        if (stopped)
            throw new NodeStoppingException("The local node is stopping. Transmission aborted.");
    }

    /**
     * @param topic Topic to which the channel is created.
     * @param nodeId Remote node id.
     * @param initMsg Channel initialization message with additional params.
     * @param channel Channel instance.
     */
    public void onChannelOpened(Object topic, UUID nodeId, SessionChannelMessage initMsg, Channel channel) {
        FileTransmitHandler session = topicHandlerMap.get(topic);

        if (session == null)
            return;

        FileIoReadContext readCtx = sesCtxMap.computeIfAbsent(topic, t -> new FileIoReadContext(nodeId, session));

        InputTransmitChannel in = null;
        OutputTransmitChannel out = null;

        try {
            in = new InputTransmitChannel(log, (SocketChannel)channel);
            out = new OutputTransmitChannel(log, (SocketChannel)channel);

            // Do not allow multiple connection for the same session id;
            if (!readCtx.inProgress.compareAndSet(false, true)) {
                RemoteHandlerException ex = new RemoteHandlerException("Current topic is already being handled by " +
                    "another thread. Channel will be closed [initMsg=" + initMsg + ", channel=" + channel +
                    ", fromNodeId=" + nodeId + ']');

                out.writeMeta(new TransmitMeta(ex));

                U.warn(log, ex);

                return;
            }

            if (!busyLock.enterBusy())
                return;

            try {
                IgniteUuid newSesId = Objects.requireNonNull(initMsg.sesId());

                if (readCtx.sesId == null)
                    readCtx.sesId = newSesId;
                else if (!readCtx.sesId.equals(newSesId)) {
                    // Attempt to receive file with new session id. Context must be reinited,
                    // previous session must be failed.
                    readCtx.session.onException(new IgniteCheckedException("The handler has been aborted " +
                        "by transfer attempt with a new sessionId: " + newSesId));

                    readCtx = new FileIoReadContext(nodeId, session);
                    readCtx.sesId = newSesId;
                    readCtx.inProgress.set(true);

                    sesCtxMap.put(topic, readCtx);
                }

                readCtx.currInChannel = in;
                readCtx.currOutChannel = out;

                // Send previous context state to sync remote and local node (on manager connected).
                if (readCtx.chunkedObj == null)
                    out.writeMeta(new TransmitMeta(readCtx.lastSeenErr));
                else {
                    final InputChunkedObject obj = readCtx.chunkedObj;

                    out.writeMeta(new TransmitMeta(obj.name(),
                        obj.startPosition() + obj.transferred(),
                        obj.count(),
                        obj.transferred() == 0,
                        obj.params(),
                        readCtx.lastSeenErr));
                }

                try {
                    // Init handler.
                    if (readCtx.started.compareAndSet(false, true))
                        readCtx.session.onBegin(nodeId);
                }
                catch (Throwable t) {
                    readCtx.started.set(false);

                    throw t;
                }
            }
            catch (Throwable t) {
                readCtx.inProgress.set(false);

                throw t;
            }
            finally {
                busyLock.leaveBusy();
            }

            onChannelOpened0(topic, readCtx);
        }
        catch (Throwable t) {
            log.error("The download session cannot be finished due to unexpected error " +
                "[ctx=" + readCtx + ", sesId=" + readCtx.sesId + ']', t);

            readCtx.lastSeenErr = new RemoteHandlerException("Error channel processing [nodeId=" + nodeId + ']', t);

            readCtx.session.onException(t);
        }
        finally {
            U.closeQuiet(in);
            U.closeQuiet(out);
            U.closeQuiet(channel);
        }
    }

    /**
     * @param topic The {@link GridTopic} to register handler to.
     * @param session The session will be created for a new channel opened.
     */
    public void addFileTransmitHandler(Object topic, FileTransmitHandler session) {
        FileTransmitHandler hdlr = topicHandlerMap.putIfAbsent(topic, session);

        if (hdlr != null)
            U.warn(log, "The topic already have an appropriate session handler [topic=" + topic + ']');
    }

    /**
     * @param topic The topic to erase handler from.
     */
    public void removeTransmitSessionHandler(Object topic) {
        topicHandlerMap.remove(topic);
    }

    /**
     * @param factory A new factory instance to set.
     */
    void chunkedStreamFactory(ChunkedObjectFactory factory) {
        streamFactory = factory;
    }

    /**
     * @param topic Topic handler related to.
     * @param readCtx The handler read context.
     * @throws Exception If processing fails.
     */
    private void onChannelOpened0(Object topic, FileIoReadContext readCtx) throws Exception {
        InputChunkedObject inChunkedObj = null;

        try {
            while (!Thread.currentThread().isInterrupted()) {
                checkNotStopped();

                // Read current stream session policy
                int plcInt = readCtx.currInChannel.readInt();

                if (plcInt == FILE_WRITER_CLOSED) {
                    readCtx.session.onEnd(readCtx.nodeId);

                    sesCtxMap.remove(topic);

                    break;
                }

                if (plcInt < 0 || plcInt > ReadPolicy.values().length)
                    throw new IOException("The policy received from channel is unknown [order=" + plcInt + ']');

                readCtx.currPlc = ReadPolicy.values()[plcInt];

                long startTime = U.currentTimeMillis();

                if (readCtx.chunkedObj == null) {
                    readCtx.chunkedObj = streamFactory.createInputChunkedObject(readCtx.nodeId,
                        readCtx.currPlc,
                        readCtx.session,
                        ioChunkSize);
                }

                inChunkedObj = readCtx.chunkedObj;

                inChunkedObj.setup(readCtx.currInChannel);

                boolean acquired;

                // Read data from the input.
                while (inChunkedObj.hasNextChunk()) {
                    if (Thread.currentThread().isInterrupted())
                        throw new InterruptedException("The thread has been interrupted. Stop processing input stream.");

                    checkNotStopped();

                    // If the limit of permits at appropriate period of time reached,
                    // the furhter invocations of the #acuqire(int) method will be blocked.
                    acquired = inBytePermits.tryAcquire(readCtx.chunkedObj.chunkSize(),
                        DFLT_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                    if (!acquired) {
                        throw new TransmitException("Download speed is too slow " +
                            "[downloadSpeed=" + inBytePermits.permitsPerSec() + " byte/sec]");
                    }

                    inChunkedObj.readChunk(readCtx.currInChannel);
                }

                inChunkedObj.close();

                readCtx.chunkedObj = null;

                // Write chunked object processing ack.
                readCtx.currOutChannel.acknowledge(inChunkedObj.count());

                long downloadTime = U.currentTimeMillis() - startTime;

                U.log(log, "The file has been successfully downloaded " +
                    "[name=" + inChunkedObj.name() + ", transferred=" + inChunkedObj.transferred() + " bytes" +
                    ", time=" + (double)((downloadTime) / 1000) + " sec" +
                    ", speed=" + inBytePermits.permitsPerSec() + " byte/sec" +
                    ", retries=" + readCtx.retries);
            }
        }
        catch (TransmitException e) {
            // Waiting for re-establishing connection.
            log.warning("The connection lost. Waiting for the new one to continue file upload " +
                "[sesId=" + readCtx.sesId + ']', e);

            readCtx.retries++;

            if (readCtx.retries == retryCnt) {
                IOException ex = new IOException("The number of retry attempts exceeded the limit. " +
                    "Max attempts: " + retryCnt, e);

                readCtx.session.onException(ex);
            }
        }
        finally {
            readCtx.inProgress.set(false);

            U.closeQuiet(inChunkedObj);
        }
    }

    /**
     * @param remoteId The remote note to connect to.
     * @param topic The remote topic to connect to.
     * @return The channel instance to communicate with remote.
     */
    public FileWriter openFileWriter(UUID remoteId, Object topic) {
        return new ChunkedFileWriter(remoteId, topic);
    }

    /**
     * Read context holds all the information about current transfer read from channel process.
     */
    private static class FileIoReadContext {
        /** The remote node input channel came from. */
        private final UUID nodeId;

        /** Handler currently in use flag. */
        private final AtomicBoolean inProgress = new AtomicBoolean();

        /** Current sesssion handler. */
        @GridToStringExclude
        private final FileTransmitHandler session;

        /** Flag indicates session started. */
        private final AtomicBoolean started = new AtomicBoolean();

        /** Unique session request id. */
        private IgniteUuid sesId;

        /** The number of retry attempts of current session to wait. */
        private int retries;

        /** The currently used input channel (updated on reconnect). */
        @GridToStringExclude
        private InputTransmitChannel currInChannel;

        /** The currently used output channel (updated on reconnect). */
        @GridToStringExclude
        private OutputTransmitChannel currOutChannel;

        /** Read policy of the way of handling input data. */
        private ReadPolicy currPlc;

        /** Last infinished downloading object. */
        private InputChunkedObject chunkedObj;

        /** Last error occurred while channel is processed by registered session handler. */
        private RemoteHandlerException lastSeenErr;

        /**
         * @param nodeId Remote node id.
         * @param session Channel handler of current topic.
         */
        public FileIoReadContext(UUID nodeId, FileTransmitHandler session) {
            this.nodeId = nodeId;
            this.session = session;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileIoReadContext.class, this);
        }
    }

    /**
     * Implementation of file writer to transfer files with the zero-copy algorithm
     * (used the {@link InputChunkedFile} under the hood).
     */
    private class ChunkedFileWriter implements FileWriter {
        /** Remote node id to connect to. */
        private final UUID remoteId;

        /** Remote topic to connect to. */
        private final Object topic;

        /** Current unique session id to transfer files. */
        private IgniteUuid sesId;

        /** Data output channel. */
        private OutputTransmitChannel out;

        /** Data intput channel. */
        private InputTransmitChannel in;

        /**
         * @param remoteId The remote note to connect to.
         * @param topic The remote topic to connect to.
         */
        public ChunkedFileWriter(
            UUID remoteId,
            Object topic
        ) {
            this.remoteId = remoteId;
            this.topic = topic;
            this.sesId = IgniteUuid.randomUuid();
        }

        /**
         * @return The syncronization meta if case connection has been reset.
         * @throws IgniteCheckedException If fails.
         */
        public TransmitMeta connect() throws IgniteCheckedException {
            try {
                Channel socket = openClsr.apply(remoteId, topic, new SessionChannelMessage(sesId))
                    .get();

                out = new OutputTransmitChannel(log, (SocketChannel)socket);
                in = new InputTransmitChannel(log, (SocketChannel)socket);

                // Synchronize state between remote and local nodes.
                TransmitMeta syncMeta = new TransmitMeta();

                in.readMeta(syncMeta);

                return syncMeta;
            }
            catch (ClusterTopologyCheckedException e) {
                throw e;
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteCheckedException("Unable to initialize an i\\o channel connection to the remote node " +
                    "[remoteId=" + remoteId + ", topic=" + topic + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void write(
            File file,
            long offset,
            long count,
            Map<String, Serializable> params,
            ReadPolicy plc
        ) throws IgniteCheckedException {
            int retries = 0;

            OutputChunkedFile outChunkedObj = new OutputChunkedFile(file, offset, count, ioChunkSize, params);

            try {
                if (log.isDebugEnabled())
                    log.debug("Start writing file to remote node [file=" + file.getName() +
                        ", rmtNodeId=" + remoteId + ", topic=" + topic + ']');

                long startTime = U.currentTimeMillis();

                while (true) {
                    if (Thread.currentThread().isInterrupted())
                        throw new InterruptedException("The thread has been interrupted. Stop uploading file.");

                    if (retries > retryCnt)
                        throw new IOException("The number of retry attempts exceeded the limit: " + retryCnt);

                    checkNotStopped();

                    try {
                        if (out == null && in == null) {
                            TransmitMeta syncMeta = connect();

                            // Stop in case of any error occurred on remote node during file processing.
                            if (syncMeta.error() != null)
                                throw syncMeta.error();

                            // If not the initial connection for the current session.
                            if (!TransmitMeta.DFLT_TRANSMIT_META.equals(syncMeta)) {
                                long transferred = syncMeta.offset() - outChunkedObj.startPosition();

                                assert transferred >= 0 : "Incorrect sync meta [offset=" + syncMeta.offset() +
                                    ", startPos=" + outChunkedObj.startPosition() + ']';
                                assert outChunkedObj.name().equals(syncMeta.name()) : "Attempt to transfer different file " +
                                    "while previous is not completed [curr=" + outChunkedObj.name() + ", meta=" + syncMeta + ']';

                                outChunkedObj.transferred(transferred);
                            }
                        }

                        // Write the policy how to handle input data.
                        out.writeInt(plc.ordinal());

                        outChunkedObj.setup(out);

                        boolean acquired;

                        while (outChunkedObj.hasNextChunk()) {
                            if (Thread.currentThread().isInterrupted())
                                throw new InterruptedException("The thread has been interrupted. Stop uploading file.");

                            checkNotStopped();

                            // If the limit of permits at appropriate period of time reached,
                            // the furhter invocations of the #acuqire(int) method will be blocked.
                            acquired = outBytePermits.tryAcquire(outChunkedObj.chunkSize(),
                                DFLT_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                            if (!acquired) {
                                throw new TransmitException("Upload speed is too slow " +
                                    "[uploadSpeed=" + inBytePermits.permitsPerSec() + " byte/sec]");
                            }

                            outChunkedObj.writeChunk(out);
                        }

                        outChunkedObj.close();

                        in.acknowledge();

                        break;
                    }
                    catch (TransmitException e) {
                        closeChannelQuiet();

                        // Re-establish the new connection to continue upload.
                        U.warn(log, "Exception while writing file to remote node. Re-establishing connection " +
                            " [remoteId=" + remoteId + ", file=" + file.getName() + ", sesId=" + sesId +
                            ", retries=" + retries + ", transferred=" + outChunkedObj.transferred() +
                            ", count=" + outChunkedObj.count() + ']', e);

                        retries++;
                    }
                }

                long uploadTime = U.currentTimeMillis() - startTime;

                U.log(log, "The file uploading operation has been completed " +
                    "[name=" + file.getName() + ", uploadTime=" + (double)((uploadTime) / 1000) + " sec" +
                    ", retries=" + retries + ']');

            }
            catch (Exception e) {
                closeChannelQuiet();

                throw new IgniteCheckedException("Exception while uploading file to the remote node. The process stopped " +
                    "[remoteId=" + remoteId + ", file=" + file.getName() + ", sesId=" + sesId + ']', e);
            }
            finally {
                U.closeQuiet(outChunkedObj);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            try {
                if (out != null) {
                    U.log(log, "Writing tombstone on close write session");

                    out.writeInt(FILE_WRITER_CLOSED);
                }
            }
            catch (IOException e) {
                U.warn(log, "The excpetion of writing 'tombstone' on channel close operation has been ignored", e);
            }
            finally {
                closeChannelQuiet();
            }
        }

        /**
         * Close channel and relese resources.
         */
        private void closeChannelQuiet() {
            U.closeQuiet(out);
            U.closeQuiet(in);

            out = null;
            in = null;
        }
    }
}
