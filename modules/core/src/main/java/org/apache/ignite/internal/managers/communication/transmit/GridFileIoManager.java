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
import org.apache.ignite.internal.managers.communication.transmit.channel.RemoteTransmitException;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitInputChannel;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitMeta;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitOutputChannel;
import org.apache.ignite.internal.managers.communication.transmit.chunk.ChunkedFileStream;
import org.apache.ignite.internal.managers.communication.transmit.chunk.ChunkedInputStream;
import org.apache.ignite.internal.managers.communication.transmit.chunk.ChunkedOutputStream;
import org.apache.ignite.internal.managers.communication.transmit.chunk.ChunkedStreamFactory;
import org.apache.ignite.internal.managers.communication.transmit.util.ByteUnit;
import org.apache.ignite.internal.managers.communication.transmit.util.InitChannelMessage;
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
    public static final int DFLT_CHUNK_SIZE_BYTES = (int)ByteUnit.BYTE.convertFrom(256, ByteUnit.KB);

    /** Reconnect attempts count to send single file (value is {@code 5}). */
    public static final int DFLT_RECONNECT_CNT = 5;

    /** The default file limit transmittion rate per node instance (value is {@code 500 MB/sec}). */
    public static final long DFLT_RATE_LIMIT_BYTES = ByteUnit.BYTE.convertFrom(500, ByteUnit.MB);

    /** The default timeout for waiting the permits. */
    private static final int DFLT_ACQUIRE_TIMEOUT_MS = 5000;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Grid logger. */
    private final IgniteLogger log;

    /** */
    private final ConcurrentMap<Object, TransmitSessionHandler> topicHandlerMap = new ConcurrentHashMap<>();

    /** The map of already known channel read contexts by its registered topics. */
    private final ConcurrentMap<Object, FileIoReadContext> sesCtxMap = new ConcurrentHashMap<>();

    /** The busy lock. */
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
    private volatile ChunkedStreamFactory streamFactory = new ChunkedStreamFactory();

    /** The maximum number of reconnect attempts (read or write attempts). */
    private volatile int reconnectCnt = DFLT_RECONNECT_CNT;

    /** The size of stream chunks. */
    private int ioStreamChunkSize = DFLT_CHUNK_SIZE_BYTES;

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
     * Get the maximum number of reconnect attempts used for uploading or downloading file when the socket connection
     * has been dropped by network issues.
     *
     * @return The number of reconnect attempts.
     */
    public int reconnectCnt() {
        return reconnectCnt;
    }

    /**
     * Set the maximum number of reconnect attempts used for uploading or downloading file when the socket connection
     * has been dropped by network issues.
     *
     * @param reconnectCnt The number of reconnect attempts.
     */
    public void reconnectCnt(int reconnectCnt) {
        this.reconnectCnt = reconnectCnt;
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
     * @param count New count of transfer speed per second.
     * @param unit The unit type of {@code count} transfer speed.
     */
    public void downloadRate(int count, ByteUnit unit) {
        if (count <= 0)
            inBytePermits.permitsPerSec(TimedSemaphore.UNLIMITED_PERMITS);
        else
            inBytePermits.permitsPerSec(unit.toBytes(count));

        U.log(log, "The file download speed has been set to: " + count + " " + unit.name() + " per sec.");
    }

    /**
     * @param unit The unit to convert download rate to.
     * @return The total file download rate, by default {@link #DFLT_RATE_LIMIT_BYTES} or
     * {@link TimedSemaphore#UNLIMITED_PERMITS} if there is no limit.
     */
    public long downloadRate(ByteUnit unit) {
        return permitsToRate(inBytePermits, unit);
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
     * @param count New count of transfer speed per second.
     * @param unit The unit type of {@code count} transfer speed.
     */
    public void uploadRate(int count, ByteUnit unit) {
        if (count <= 0)
            outBytePermits.permitsPerSec(TimedSemaphore.UNLIMITED_PERMITS);
        else
            outBytePermits.permitsPerSec(unit.toBytes(count));

        U.log(log, "The file upload speed has been set to: " + count + " " + unit.name() + " per sec.");
    }

    /**
     * @param unit The unit to convert download rate to.
     * @return The total file upload rate, by default {@link #DFLT_RATE_LIMIT_BYTES} or
     * {@link TimedSemaphore#UNLIMITED_PERMITS} if there is no limit.
     */
    public long uploadRate(ByteUnit unit) {
        return permitsToRate(outBytePermits, unit);
    }

    /**
     * @param semaphore The semaphore to convert permits.
     * @param unit The unit to convert to.
     * @return The rate amount or {@link TimedSemaphore#UNLIMITED_PERMITS} in there is no limit.
     */
    private long permitsToRate(TimedSemaphore semaphore, ByteUnit unit) {
        return semaphore.permitsPerSec() == TimedSemaphore.UNLIMITED_PERMITS ?
            TimedSemaphore.UNLIMITED_PERMITS : ByteUnit.BYTE.convertTo(semaphore.permitsPerSec(), unit);
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
    public void onChannelOpened(Object topic, UUID nodeId, InitChannelMessage initMsg, Channel channel) {
        TransmitSessionHandler session = topicHandlerMap.get(topic);

        if (session == null)
            return;

        FileIoReadContext readCtx = sesCtxMap.computeIfAbsent(topic, t -> new FileIoReadContext(nodeId, session));

        // Do not allow multiple connection for the same session id;
        if (!readCtx.inProgress.compareAndSet(false, true)) {
            U.warn(log, "Current topic is already being handled. Opened channel will " +
                "be closed [initMsg=" + initMsg + ", channel=" + channel + ']');

            return;
        }

        IgniteUuid sesId = null;

        try {
            if (!busyLock.enterBusy())
                return;

            try {
                sesId = Objects.requireNonNull(initMsg.sesId());

                readCtx.currInChannel = new TransmitInputChannel(ctx, (SocketChannel)channel);
                readCtx.currOutChannel = new TransmitOutputChannel(ctx, (SocketChannel)channel);

                if (readCtx.started.compareAndSet(false, true))
                    readCtx.session.begin(nodeId, sesId.toString());
            }
            finally {
                busyLock.leaveBusy();
            }

            onChannelOpened0(sesId, readCtx);
        }
        catch (Throwable t) {
            log.error("Error processing channel creation event [topic=" + topic +
                ", channel=" + channel + ", sessionId=" + sesId + ']', t);

            if (readCtx != null)
                readCtx.session.onException(t);
        }
        finally {
            readCtx.inProgress.compareAndSet(true, false);
            U.closeQuiet(channel);
        }
    }

    /**
     * @param topic The {@link GridTopic} to register handler to.
     * @param session The session will be created for a new channel opened.
     */
    public void addTransmitSessionHandler(Object topic, TransmitSessionHandler session) {
        TransmitSessionHandler hdlr = topicHandlerMap.putIfAbsent(topic, session);

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
    void chunkedStreamFactory(ChunkedStreamFactory factory) {
        streamFactory = factory;
    }

    /**
     * @param tctx The handler read context.
     */
    private void onChannelOpened0(Object topic, FileIoReadContext tctx) {
        ChunkedInputStream inStream = null;
        TransmitMeta meta = null;

        try {
            // Send previous meta to sync remote and local file transferred states.
            if (tctx.stream == null)
                tctx.currOutChannel.writeMeta(TransmitMeta.DFLT_TRANSMIT_META);
            else
                tctx.currOutChannel.writeMeta(tctx.stream.meta());

            while (!Thread.currentThread().isInterrupted()) {
                checkNotStopped();

                // Read current stream session policy
                ReadPolicy currPlc = tctx.currInChannel.readPolicy();

                if (currPlc == ReadPolicy.NONE) {
                    tctx.session.end();

                    sesCtxMap.remove(topic);

                    break;
                }

                long startTime = U.currentTimeMillis();

                tctx.currPlc = currPlc;

                if (tctx.stream == null)
                    tctx.stream = streamFactory.createInputStream(tctx.currPlc, tctx.session, ioStreamChunkSize);

                inStream = tctx.stream;

                inStream.setup(tctx.currInChannel);

                // Read data from the input.
                while (!inStream.endStream()) {
                    if (Thread.currentThread().isInterrupted())
                        throw new InterruptedException("The thread has been interrupted. Stop processing input stream.");

                    checkNotStopped();

                    // If the limit of permits at appropriate period of time reached,
                    // the furhter invocations of the #acuqire(int) method will be blocked.
                    boolean acquired = inBytePermits.tryAcquire(tctx.stream.chunkSize(),
                        DFLT_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                    if (!acquired)
                        throw new RemoteTransmitException("Download speed is too slow " +
                            "[rate=" + ByteUnit.BYTE.toKB(inBytePermits.permitsPerSec()) + " Kb/sec]");

                    inStream.readChunk(tctx.currInChannel);
                }

                inStream.checkStreamEOF();
                inStream.close();

                tctx.stream = null;

                // Write stream processing acknowledge.
                tctx.currOutChannel.acknowledge(inStream.count());

                long downloadTime = U.currentTimeMillis() - startTime;

                U.log(log, "The file has been successfully downloaded " +
                    "[name=" + inStream.name() + ", transferred=" + ByteUnit.BYTE.toKB(inStream.transferred()) + " Kb" +
                    ", time=" + (double)((downloadTime) / 1000) + " sec" +
                    ", rate=" + ByteUnit.BYTE.toKB(inBytePermits.permitsPerSec()) + " Kb/sec" +
                    ", reconnects=" + tctx.reconnects);
            }
        }
        catch (RemoteTransmitException e) {
            // Waiting for re-establishing connection.
            log.warning("The connection lost. Waiting for the new one to continue file upload", e);

            tctx.reconnects++;

            if (tctx.reconnects == reconnectCnt) {
                IOException ex = new IOException("The number of reconnect attempts exceeded the limit. " +
                    "Max attempts: " + reconnectCnt, e);

                tctx.session.onException(ex);
            }
        }
        catch (Throwable t) {
            log.error("The download session cannot be finished due to unexpected error [ctx=" + tctx +
                ", channel=" + tctx.currInChannel + ", lastMeta=" + meta + ']', t);

            tctx.session.onException(t);
        }
        finally {
            U.closeQuiet(inStream);
        }
    }

    /**
     * @param remoteId The remote note to connect to.
     * @param topic The remote topic to connect to.
     * @return The channel instance to communicate with remote.
     */
    public FileWriter openFileWriter(UUID remoteId, Object topic) {
        return new FileWriterImpl(remoteId, topic);
    }

    /**
     *
     */
    private class FileWriterImpl implements FileWriter {
        /** */
        private final UUID remoteId;

        /** */
        private final Object topic;

        /** */
        private IgniteUuid sesId;

        /** */
        private TransmitOutputChannel out;

        /** */
        private TransmitInputChannel in;

        /**
         * @param remoteId The remote note to connect to.
         * @param topic The remote topic to connect to.
         */
        public FileWriterImpl(
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
                Channel socket = openClsr.apply(remoteId, topic, new InitChannelMessage(sesId))
                    .get();

                out = new TransmitOutputChannel(ctx, (SocketChannel)socket);
                in = new TransmitInputChannel(ctx, (SocketChannel)socket);

                // Synchronize state between remote and local nodes.
                TransmitMeta syncMeta = new TransmitMeta();

                in.readMeta(syncMeta);

                return syncMeta;
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
            int reconnects = 0;

            ChunkedOutputStream fileStream = new ChunkedFileStream(
                new FileHandler() {
                    @Override public String begin(
                        String name,
                        long pos,
                        long cnt,
                        Map<String, Serializable> params
                    ) {
                        return file.getAbsolutePath();
                    }

                    @Override public void end(File file, Map<String, Serializable> params) {
                        if (log.isDebugEnabled())
                            log.debug("File has been successfully uploaded: " + file.getName());
                    }
                },
                file.getName(),
                offset,
                count,
                ioStreamChunkSize,
                params);

            try {
                if (log.isDebugEnabled())
                    log.debug("Start writing file to remote node [file=" + file.getName() +
                        ", rmtNodeId=" + remoteId + ", topic=" + topic + ']');

                long startTime = U.currentTimeMillis();

                while (true) {
                    if (Thread.currentThread().isInterrupted())
                        throw new InterruptedException("The thread has been interrupted. Stop uploading file.");

                    if (reconnects > reconnectCnt)
                        throw new IOException("The number of reconnect attempts exceeded the limit: " + reconnectCnt);

                    checkNotStopped();

                    try {
                        if (out == null && in == null) {
                            TransmitMeta syncMeta = connect();

                            // If not the initial connection for the current session.
                            if (!TransmitMeta.DFLT_TRANSMIT_META.equals(syncMeta)) {
                                long transferred = syncMeta.offset() - fileStream.startPosition();

                                assert transferred >= 0 : "Incorrect sync meta [offset=" + syncMeta.offset() +
                                    ", startPos=" + fileStream.startPosition() + ']';
                                assert fileStream.name().equals(syncMeta.name()) : "Attempt to transfer different file " +
                                    "while previous is not completed [curr=" + fileStream.name() + ", meta=" + syncMeta + ']';

                                fileStream.transferred(transferred);
                            }
                        }

                        // Write the policy how to handle input data.
                        out.writePolicy(plc);

                        fileStream.setup(out);

                        while (!fileStream.endStream()) {
                            if (Thread.currentThread().isInterrupted())
                                throw new InterruptedException("The thread has been interrupted. Stop uploading file.");

                            checkNotStopped();

                            // If the limit of permits at appropriate period of time reached,
                            // the furhter invocations of the #acuqire(int) method will be blocked.
                            boolean acquired = outBytePermits.tryAcquire(fileStream.chunkSize(),
                                DFLT_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                            if (!acquired)
                                throw new RemoteTransmitException("Upload speed is too slow " +
                                    "[rate=" + ByteUnit.BYTE.toKB(inBytePermits.permitsPerSec()) + " Kb/sec]");

                            fileStream.writeChunk(out);
                        }

                        fileStream.close();

                        in.acknowledge();

                        break;
                    }
                    catch (ClusterTopologyCheckedException e) {
                        // Node left the grid, no reason to reconnect.
                        throw e;
                    }
                    catch (IOException | IgniteCheckedException e) {
                        closeChannelQuiet();

                        // Re-establish the new connection to continue upload.
                        U.warn(log, "Exception while writing file to remote node. Re-establishing connection " +
                            " [remoteId=" + remoteId + ", file=" + file.getName() + ", sesId=" + sesId +
                            ", reconnects=" + reconnects + ", transferred=" + fileStream.transferred() +
                            ", count=" + fileStream.count() + ']', e);

                        reconnects++;
                    }
                }

                long uploadTime = U.currentTimeMillis() - startTime;

                U.log(log, "The file uploading operation has been completed " +
                    "[name=" + file.getName() + ", uploadTime=" + (double)((uploadTime)/1000) + " sec" +
                    ", reconnects=" + reconnects + ']');

            }
            catch (Exception e) {
                closeChannelQuiet();

                throw new IgniteCheckedException("Exception while uploading file to the remote node. The process stopped " +
                    "[remoteId=" + remoteId + ", file=" + file.getName() + ", sesId=" + sesId + ']', e);
            }
            finally {
                U.closeQuiet(fileStream);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            try {
                if (out != null) {
                    U.log(log, "Writing tombstone on close write session");

                    out.writePolicy(ReadPolicy.NONE);
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

    /**
     *
     */
    private static class FileIoReadContext {
        /** The remote node input channel came from. */
        private final UUID nodeId;

        /** Handler currently in use flag. */
        private final AtomicBoolean inProgress = new AtomicBoolean();

        /** Current sesssion. */
        @GridToStringExclude
        private final TransmitSessionHandler session;

        /** Flag indicates session started. */
        private final AtomicBoolean started = new AtomicBoolean();

        /** The number of reconnect attempts of current session. */
        private int reconnects;

        /** The currently used input channel (updated on reconnect). */
        @GridToStringExclude
        private TransmitInputChannel currInChannel;

        /** The currently used output channel (updated on reconnect). */
        @GridToStringExclude
        private TransmitOutputChannel currOutChannel;

        /** The read policy of handlind input data. */
        private ReadPolicy currPlc;

        /** The last infinished download. */
        private ChunkedInputStream stream;

        /**
         * @param nodeId The remote node id.
         * @param session The channel handler.
         */
        public FileIoReadContext(UUID nodeId, TransmitSessionHandler session) {
            this.nodeId = nodeId;
            this.session = session;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileIoReadContext.class, this);
        }
    }
}
