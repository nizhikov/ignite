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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitException;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitMeta;
import org.apache.ignite.internal.managers.communication.transmit.chunk.InputChunkedBuffer;
import org.apache.ignite.internal.managers.communication.transmit.chunk.InputChunkedFile;
import org.apache.ignite.internal.managers.communication.transmit.chunk.InputChunkedObject;
import org.apache.ignite.internal.managers.communication.transmit.chunk.OutputChunkedFile;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T2;
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

    /** Default transmit meta. */
    private static final String DFLT_TRANSMIT_META = "default";

    /** Session end meta. */
    private static final String DFLT_SESSION_CLOSE_META = "close";

    /** Default timeout in milleseconds to wait an IO data on socket. See Socket#setSoTimeout(int). */
    private static final int DFLT_IO_TIMEOUT_MS = 5_000;

    /**
     * A connection reset by peer message means that the node we are connected to has reset the connection.
     * This is usually caused by a high amount of traffic on the host, but may be caused by a server error as well.
     */
    private static final String RESET_BY_PEER_MSG = "Connection reset by peer";

    /**
     * A message means that the remote node closed the connection by sending a TCP/IP RST packet.
     * This is usually caused by: sending malformed data, the network link between nodes is going
     * down for some reason, the remote node is crashed by a bug, the remote node has exhausted
     * system resources.
     */
    private static final String CLOSED_BY_REMOTE_MSG = "An existing connection was forcibly closed by the remote host";

    /**
     * A message means that at some point of time remote node decides simply to drop the connection. It can
     * happen while the local node is still sending data on the connection, and still able to do so. When the
     * local node attempts to get the next response from remote, it fails with such error.
     */
    private static final String ABORTED_BY_SOFTWARE_MSG = "An established connection was aborted by the software";

    /** Ignite kernal context. */
    private final GridKernalContext ctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Map of registered handlers per each IO topic. */
    private final ConcurrentMap<Object, TransmissionHandler> topicHandlerMap = new ConcurrentHashMap<>();

    /** The map of already known channel read contexts by its registered topics. */
    private final ConcurrentMap<Object, FileIoReadContext> sesCtxMap = new ConcurrentHashMap<>();

    /** The map of sessions which are currently writing files and their corresponding interruption flags. */
    private final ConcurrentMap<T2<UUID, IgniteUuid>, AtomicBoolean> writeSesInterruptMap = new ConcurrentHashMap<>();

    /** Managers busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Closure to open a new channel to remote node. */
    private final IgniteTriClosure<UUID, Object, Message, IgniteInternalFuture<Channel>> openChannelClsr;

    /** The factory produces chunked objects to process an input data channel. */
    private IgniteTriClosure<UUID, TransmissionHandler, TransmitMeta, InputChunkedObject> chunkedObjFactory;

    /** Listener to handle NODE_LEFT, NODE_FAIL events while waiting for remote reconnects. */
    private DiscoveryEventListener discoLsnr;

    /** Processor stopping flag. */
    private volatile boolean stopped;

    /** The maximum number of retry attempts (read or write attempts). */
    private volatile int retryCnt = DFLT_RETRY_CNT;

    /** The size of each chunks of chunked objects. */
    private int chunkSize = DFLT_CHUNK_SIZE_BYTES;

    /**
     * @param ctx Kernal context.
     */
    public GridFileIoManager(
        GridKernalContext ctx,
        IgniteTriClosure<UUID, Object, Message, IgniteInternalFuture<Channel>> openChannelClsr
    ) {
        this.ctx = ctx;
        log = ctx.log(getClass());
        this.openChannelClsr = openChannelClsr;

        chunkedObjFactory = this::createChunkedObject;
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

                // Stop all writer sessions.
                for (Map.Entry<T2<UUID, IgniteUuid>, AtomicBoolean> writeSesEntry: writeSesInterruptMap.entrySet()) {
                    if (writeSesEntry.getKey().get1().equals(leftNodeId))
                        writeSesEntry.getValue().set(true);
                }

                // Clear the context on the uploader node left.
                for (Map.Entry<Object, FileIoReadContext> sesEntry : sesCtxMap.entrySet()) {
                    FileIoReadContext ioctx = sesEntry.getValue();

                    if (ioctx.nodeId.equals(leftNodeId)) {
                        ioctx.hndlr.onException(leftNodeId, new ClusterTopologyCheckedException("Failed to proceed download. " +
                            "The remote node node left the grid: " + leftNodeId));

                        ioctx.interrupted = true;

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
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * Check the node is stopping.
     */
    private void checkNotStopped() throws NodeStoppingException {
        if (stopped)
            throw new NodeStoppingException("Operation has been cancelled (node is stopping)");
    }

    /**
     * @param topic Topic to which the channel is created.
     * @param nodeId Remote node id.
     * @param initMsg Channel initialization message with additional params.
     * @param channel Channel instance.
     */
    public void onChannelOpened(Object topic, UUID nodeId, SessionChannelMessage initMsg, SocketChannel channel) {
        FileIoReadContext readCtx = null;
        IgniteUuid newSesId = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;

        try {
            TransmissionHandler session = topicHandlerMap.get(topic);

            if (session == null)
                return;

            if (initMsg == null || initMsg.sesId() == null)
                return;

            readCtx = sesCtxMap.computeIfAbsent(topic, t -> new FileIoReadContext(nodeId, session));

            configureBlocking(channel);

            in = new ObjectInputStream(channel.socket().getInputStream());
            out = new ObjectOutputStream(channel.socket().getOutputStream());

            // Do not allow multiple connection for the same session id;
            if (!readCtx.inProgress.compareAndSet(false, true)) {
                IgniteCheckedException ex;

                U.warn(log, ex = new IgniteCheckedException("Current topic is already being handled by " +
                    "another thread. Channel will be closed [initMsg=" + initMsg + ", channel=" + channel +
                    ", fromNodeId=" + nodeId + ']'));

                TransmitMeta exMeta = new TransmitMeta(DFLT_TRANSMIT_META, ex);

                exMeta.writeExternal(out);

                return;
            }

            if (!busyLock.enterBusy())
                return;

            try {
                newSesId = initMsg.sesId();

                if (readCtx.sesId == null)
                    readCtx.sesId = newSesId;
                else if (!readCtx.sesId.equals(newSesId)) {
                    // Attempt to receive file with new session id. Context must be reinited,
                    // previous session must be failed.
                    readCtx.hndlr.onException(nodeId, new IgniteCheckedException("The handler has been aborted " +
                        "by transfer attempt with a new sessionId: " + newSesId));

                    readCtx = new FileIoReadContext(nodeId, session);
                    readCtx.sesId = newSesId;
                    readCtx.inProgress.set(true);

                    sesCtxMap.put(topic, readCtx);
                }

                TransmitMeta meta;

                // Send previous context state to sync remote and local node (on manager connected).
                if (readCtx.chunkedObj == null)
                    meta = new TransmitMeta(DFLT_TRANSMIT_META, readCtx.lastSeenErr);
                else {
                    final InputChunkedObject obj = readCtx.chunkedObj;

                    meta = new TransmitMeta(obj.name(),
                        obj.startPosition() + obj.transferred(),
                        obj.count(),
                        obj.transferred() == 0,
                        obj.params(),
                        readCtx.currPlc,
                        readCtx.lastSeenErr);
                }

                meta.writeExternal(out);

                try {
                    // Init handler.
                    if (readCtx.started.compareAndSet(false, true))
                        readCtx.hndlr.onBegin(nodeId);
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

            onChannelOpened0(topic, readCtx, in, out, channel);
        }
        catch (Throwable t) {
            U.error(log, "The download session cannot be finished due to unexpected error " +
                "[ctx=" + readCtx + ", sesKey=" + newSesId + ']', t);

            if (readCtx != null) {
                readCtx.lastSeenErr = new IgniteCheckedException("Channel processing error [nodeId=" + nodeId + ']', t);

                readCtx.hndlr.onException(nodeId, t);
            }
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
    public void addTransmissionHandler(Object topic, TransmissionHandler session) {
        TransmissionHandler hdlr = topicHandlerMap.putIfAbsent(topic, session);

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
     * @param topic Topic handler related to.
     * @param readCtx The handler read context.
     * @throws Exception If processing fails.
     */
    private void onChannelOpened0(
        Object topic,
        FileIoReadContext readCtx,
        ObjectInputStream in,
        ObjectOutputStream out,
        ReadableByteChannel channel
    ) throws Exception {
        InputChunkedObject inChunkedObj = null;

        try {
            while (!Thread.currentThread().isInterrupted()) {
                checkNotStopped();

                TransmitMeta meta = new TransmitMeta();

                meta.readExternal(in);

                if (DFLT_SESSION_CLOSE_META.equals(meta.name())) {
                    readCtx.hndlr.onEnd(readCtx.nodeId);

                    sesCtxMap.remove(topic);

                    break;
                }

                if (readCtx.chunkedObj == null) {
                    readCtx.chunkedObj = chunkedObjFactory.apply(readCtx.nodeId,
                        readCtx.hndlr,
                        meta);

                    readCtx.currPlc = meta.policy();
                }

                long startTime = U.currentTimeMillis();

                inChunkedObj = readCtx.chunkedObj;

                inChunkedObj.setup(meta, chunkSize, () -> stopped || readCtx.interrupted);
                inChunkedObj.doRead(channel);
                inChunkedObj.close();

                readCtx.chunkedObj = null;
                readCtx.currPlc = null;

                // Write chunked object processing ack.
                out.writeLong(inChunkedObj.transferred());
                out.flush();

                long downloadTime = U.currentTimeMillis() - startTime;

                U.log(log, "The file has been successfully downloaded " +
                    "[name=" + inChunkedObj.name() + ", transferred=" + inChunkedObj.transferred() + " bytes" +
                    ", time=" + (double)((downloadTime) / 1000) + " sec" +
                    ", retries=" + readCtx.retries);
            }
        }
        catch (IOException e) {
            if (transmitIOException(e)) {
                // Waiting for re-establishing connection.
                U.warn(log, "Сonnection from the remote node lost. Will wait for the new one to continue file " +
                    "download " + "[nodeId=" + readCtx.nodeId + ", sesKey=" + readCtx.sesId + ']', e);

                readCtx.retries++;

                if (readCtx.retries == retryCnt) {
                    throw  new IOException("Number of retry attempts to download file exceeded the limit. " +
                        "Max attempts: " + retryCnt, e);
                }
            }
            else
                throw e;
        }
        finally {
            readCtx.inProgress.set(false);

            U.closeQuiet(inChunkedObj);
        }
    }

    /**
     * @param factory A new factory instance to set.
     */
    void chunkedObjectFactory(IgniteTriClosure<UUID, TransmissionHandler, TransmitMeta, InputChunkedObject> factory) {
        chunkedObjFactory = factory;
    }

    /**
     * @param nodeId Remote node id.
     * @param hndlr The current handler instance which produces file handlers.
     * @return Chunked object instance.
     * @throws IgniteCheckedException If fails.
     */
    private InputChunkedObject createChunkedObject(
        UUID nodeId,
        TransmissionHandler hndlr,
        TransmitMeta objMeta
    ) throws IgniteCheckedException {
        switch (objMeta.policy()) {
            case FILE:
                return new InputChunkedFile(
                    objMeta.name(),
                    objMeta.offset(),
                    objMeta.count(),
                    objMeta.params(),
                    hndlr.fileHandler(nodeId,
                        objMeta.name(),
                        objMeta.offset(),
                        objMeta.count(),
                        objMeta.params()));

            case BUFF:
                return new InputChunkedBuffer(
                    objMeta.name(),
                    objMeta.offset(),
                    objMeta.count(),
                    objMeta.params(),
                    hndlr.chunkHandler(nodeId,
                        objMeta.name(),
                        objMeta.offset(),
                        objMeta.count(),
                        objMeta.params()));

            default:
                throw new IgniteCheckedException("The type of read plc is unknown. The impelentation " +
                    "required: " + objMeta.policy());
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
     * @param channel Socket channel to configure blocking mode.
     * @throws IOException If fails.
     */
    private static void configureBlocking(SocketChannel channel) throws IOException {
        // Timeout must be enabled prior to entering the blocking mode to have effect.
        channel.socket().setSoTimeout(DFLT_IO_TIMEOUT_MS);
        channel.configureBlocking(true);
    }

    /**
     * @param ex IO exception to check.
     * @return {@code true} if an IOException related to connection problems.
     */
    private static boolean transmitIOException(IOException ex) {
        // The set of local issues with connection.
        if (ex instanceof TransmitException ||
            ex instanceof EOFException ||
            ex instanceof ClosedChannelException ||
            ex instanceof SocketTimeoutException ||
            ex instanceof AsynchronousCloseException) {
            // Return the new one with detailed message.
            return true;
        }
        else if (ex instanceof IOException) {
            // Improve IOException connection error handling
            String causeMsg = ex.getMessage();

            if (causeMsg == null)
                return false;

            return causeMsg.startsWith(RESET_BY_PEER_MSG) ||
                causeMsg.startsWith(CLOSED_BY_REMOTE_MSG) ||
                causeMsg.startsWith(ABORTED_BY_SOFTWARE_MSG);
        }

        return false;
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
        private final TransmissionHandler hndlr;

        /** Flag indicates session started. */
        private final AtomicBoolean started = new AtomicBoolean();

        /** Unique session request id. */
        private IgniteUuid sesId;

        /** The number of retry attempts of current session to wait. */
        private int retries;

        /** Read policy of the way of handling input data. */
        private ReadPolicy currPlc;

        /** Last infinished downloading object. */
        private InputChunkedObject chunkedObj;

        /** Last error occurred while channel is processed by registered session handler. */
        private IgniteCheckedException lastSeenErr;

        /** Flag indicates that current file handling process must be interrupted. */
        private volatile boolean interrupted;

        /**
         * @param nodeId Remote node id.
         * @param hndlr Channel handler of current topic.
         */
        public FileIoReadContext(UUID nodeId, TransmissionHandler hndlr) {
            this.nodeId = nodeId;
            this.hndlr = hndlr;
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

        /** Current unique session identifier to transfer files to remote node. */
        private T2<UUID, IgniteUuid> sesKey;

        /** Instance of opened writable channel to work with. */
        private WritableByteChannel channel;

        /** Decorated with data operations socket of output channel. */
        private ObjectOutput out;

        /** Decoreated with data operations socket of input channel. */
        private ObjectInput in;

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
            this.sesKey = new T2<>(remoteId, IgniteUuid.randomUuid());
        }

        /**
         * @return The syncronization meta if case connection has been reset.
         * @throws IgniteCheckedException If fails.
         * @throws IOException If fails.
         */
        public TransmitMeta connect() throws IgniteCheckedException, IOException {
            writeSesInterruptMap.putIfAbsent(sesKey, new AtomicBoolean());

            SocketChannel channel = (SocketChannel)openChannelClsr.apply(remoteId,
                topic,
                new SessionChannelMessage(sesKey.get2()))
                .get();

            configureBlocking(channel);

            this.channel = (WritableByteChannel)channel;
            out = new ObjectOutputStream(channel.socket().getOutputStream());
            in = new ObjectInputStream(channel.socket().getInputStream());

            // Synchronize state between remote and local nodes.
            TransmitMeta syncMeta = new TransmitMeta();

            syncMeta.readExternal(in);

            return syncMeta;
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

            OutputChunkedFile outChunkedObj = new OutputChunkedFile(file, offset, count, params, chunkSize);

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
                        long transferred = 0;

                        if (out == null && in == null) {
                            TransmitMeta syncMeta = connect();

                            // Stop in case of any error occurred on remote node during file processing.
                            if (syncMeta.error() != null)
                                throw syncMeta.error();

                            // If not the initial connection for the current session.
                            if (!DFLT_TRANSMIT_META.equals(syncMeta.name())) {
                                transferred = syncMeta.offset() - outChunkedObj.startPosition();

                                assert transferred >= 0 : "Incorrect sync meta [offset=" + syncMeta.offset() +
                                    ", startPos=" + outChunkedObj.startPosition() + ']';
                                assert outChunkedObj.name().equals(syncMeta.name()) : "Attempt to transfer different file " +
                                    "while previous is not completed [curr=" + outChunkedObj.name() + ", meta=" + syncMeta + ']';
                            }
                        }

                        outChunkedObj.setup(out,
                            transferred,
                            plc,
                            () -> stopped || writeSesInterruptMap.get(sesKey).get());

                        outChunkedObj.doWrite(channel);
                        outChunkedObj.close();

                        // Read file received acknowledge.
                        long total = in.readLong();

                        assert total == outChunkedObj.transferred();

                        break;
                    }
                    catch (IOException e) {
                        if (transmitIOException(e)) {
                            closeChannelQuiet();

                            // Re-establish the new connection to continue upload.
                            U.warn(log, "Connection lost while writing file to remote node and " +
                                "will be re-establishing [remoteId=" + remoteId + ", file=" + file.getName() +
                                ", sesKey=" + sesKey + ", retries=" + retries +
                                ", transferred=" + outChunkedObj.transferred() +
                                ", count=" + outChunkedObj.count() + ']', e);

                            retries++;
                        }
                        else
                            throw e;
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
                    "[remoteId=" + remoteId + ", file=" + file.getName() + ", sesKey=" + sesKey + ']', e);
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

                    new TransmitMeta(DFLT_SESSION_CLOSE_META, null).writeExternal(out);
                }

                writeSesInterruptMap.remove(sesKey);
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
            U.closeQuiet(channel);

            out = null;
            in = null;
            channel = null;
        }
    }
}
