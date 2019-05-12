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
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.transmit.channel.RemoteTransmitException;
import org.apache.ignite.internal.processors.transmit.channel.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.channel.TransmitMeta;
import org.apache.ignite.internal.processors.transmit.channel.TransmitOutputChannel;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedFileStream;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedInputStream;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedOutputStream;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedStream;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedStreamFactory;
import org.apache.ignite.internal.processors.transmit.util.ByteUnit;
import org.apache.ignite.internal.processors.transmit.util.TimedSemaphore;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

import static java.util.Optional.ofNullable;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 *
 */
public class IgniteFileTransmitProcessor extends GridProcessorAdapter {
    /**
     * The default transfer chunk size transfer in bytes. Setting the transfer chunk size
     * more than <tt>1 MB</tt> is meaningless because there is no asymptotic benefit.
     * What you're trying to achieve with larger transfer chunk sizes is fewer context
     * switches, and every time you double the transfer size you halve the context switch cost.
     */
    public static final int DFLT_CHUNK_SIZE_BYTES = (int)ByteUnit.BYTE.convertFrom(256, ByteUnit.KB);

    /** Reconnect attempts count to send single file. */
    public static final int DFLT_RECONNECT_CNT = 5;

    /** The default file download rate. */
    public static final long DFLT_DOWNLOAD_RATE = ByteUnit.BYTE.convertFrom(500, ByteUnit.MB);

    /** The sessionId attribute map key. */
    private static final String SESSION_ID_KEY = "sessionId";

    /** The last file offset attribute map key.*/
    private static final String RECEIVED_BYTES_KEY = "receivedBytes";

    /** The last file name attribute map key. */
    private static final String LAST_FILE_NAME_KEY = "lastFileName";

    /** */
    private final ConcurrentMap<Object, TransmitSessionFactory> topicFactoryMap = new ConcurrentHashMap<>();

    /** The map of already known channel read contexts by its session id. */
    private final ConcurrentMap<String, FileIoReadContext> sessionContextMap = new ConcurrentHashMap<>();

    /** The busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /**
     * The total amount of permits for the configured period of time. To limit the download
     * speed of reading the stream of data we should acuire a permit per byte.
     * <p>
     * For instance, for the 128 Kb/sec rate you should specify total <tt>131_072</tt> permits.
     */
    private final TimedSemaphore permitsSemaphore;

    /** */
    private DiscoveryEventListener discoLsnr;

    /** The factory produces chunked stream to process an input data channel. */
    private volatile ChunkedStreamFactory streamFactory = new ChunkedStreamFactory();

    /** The maximum number of reconnect attempts (read or write attempts). */
    private volatile int reconnectCnt = DFLT_RECONNECT_CNT;

    /** The size of stream chunks. */
    private int ioStreamChunkSize = DFLT_CHUNK_SIZE_BYTES;

    /**
     * @param ctx Kernal context.
     */
    public IgniteFileTransmitProcessor(GridKernalContext ctx) {
        super(ctx);

        permitsSemaphore = new TimedSemaphore(DFLT_DOWNLOAD_RATE);
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
     * Set the download rate per second. It is possible to modify the download rate at runtime.
     * Reducing the speed takes effect immediately by blocking incoming requests on the
     * semaphore {@link #permitsSemaphore}. If the speed is increased than waiting threads
     * are not released immediately, but will be wake up when the next time period of
     * {@link TimedSemaphore} runs out.
     * <p>
     * Setting the amount to {@link TimedSemaphore#UNLIMITED_PERMITS} will switch off the
     * configured {@link #permitsSemaphore} limit.
     *
     * @param amount The amount of transfer unit rate.
     * @param byteUnit The unit type transfer rate.
     */
    public void downloadRate(int amount, ByteUnit byteUnit) {
        if (amount <= 0)
            permitsSemaphore.permitsPerSec(TimedSemaphore.UNLIMITED_PERMITS);
        else
            permitsSemaphore.permitsPerSec(byteUnit.toBytes(amount));

        U.log(log, "The file download speed has been set to: " + amount + " " + byteUnit.name() + " per sec.");
    }

    /**
     * @param unit The unit to convert download rate to.
     * @return The total file download rate, by default {@link #DFLT_DOWNLOAD_RATE} or
     * {@link TimedSemaphore#UNLIMITED_PERMITS} if there is no limit.
     */
    public long downloadRate(ByteUnit unit) {
        return permitsSemaphore.permitsPerSec() == TimedSemaphore.UNLIMITED_PERMITS ?
            TimedSemaphore.UNLIMITED_PERMITS : ByteUnit.BYTE.convertTo(permitsSemaphore.permitsPerSec(), unit);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addDiscoveryEventListener(discoLsnr = (evt, disco) -> {
            UUID leftNodeId = evt.eventNode().id();

            // Clear the context on the uploader node left.
            for (Map.Entry<String, FileIoReadContext> sesEntry : sessionContextMap.entrySet()) {
                FileIoReadContext ioctx = sesEntry.getValue();

                if (ioctx.nodeId.equals(leftNodeId)) {
                    ClusterTopologyCheckedException ex;

                    ioctx.session.onException(ex = new ClusterTopologyCheckedException("Failed to proceed download. " +
                        "The remote node node left the grid: " + leftNodeId));

                    sessionContextMap.remove(sesEntry.getKey());
                }
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        busyLock.block();

        try {

            ctx.event().removeDiscoveryEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

            for (Object topic : topicFactoryMap.keySet())
                removeFileIoChannelHandler(topic);

            permitsSemaphore.shutdown();
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * @param topic The {@link GridTopic} to register handler to.
     * @param factory The factory will create a new handler for each created channel.
     */
    public void addFileIoChannelHandler(Object topic, TransmitSessionFactory factory) {
        if (topicFactoryMap.putIfAbsent(topic, factory) == null) {
            ctx.io().addChannelListener(topic, new GridIoChannelListener() {
                @Override public Map<String, Serializable> onChannelConfigure(IgniteSocketChannel channel) {
                    if (!busyLock.enterBusy())
                        return null;

                    try {
                        // Restore the last received session state on remote node.
                        String sessionId = channel.attr(SESSION_ID_KEY);

                        assert sessionId != null;

                        FileIoReadContext readCtx = sessionContextMap.get(sessionId);

                        if (readCtx == null)
                            return null;
                        else {
                            ChunkedStream stream = readCtx.stream;

                            if (stream == null)
                                return null;
                            else {
                                Map<String, Serializable> attrs = new HashMap<>();

                                attrs.put(RECEIVED_BYTES_KEY, stream.transferred());
                                attrs.put(LAST_FILE_NAME_KEY, stream.name());

                                return attrs;
                            }
                        }
                    }
                    finally {
                        busyLock.leaveBusy();
                    }
                }

                @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                    String sessionId = null;
                    FileIoReadContext sesCtx = null;

                    try {
                        sessionId = Objects.requireNonNull(channel.attr(SESSION_ID_KEY));

                        sesCtx = sessionContextMap.computeIfAbsent(sessionId,
                            sesId -> new FileIoReadContext(nodeId, factory.create()));

                        sesCtx.currInput = new TransmitInputChannel(ctx, channel);
                        sesCtx.currOutput = new TransmitOutputChannel(ctx, channel);

                        if (sesCtx.started.compareAndSet(false, true))
                            sesCtx.session.begin(nodeId, sessionId);

                        onChannelCreated0(sessionId, sesCtx);
                    }
                    catch (Throwable t) {
                        log.error("Error processing channel creation event [topic=" + topic +
                            ", channel=" + channel + ", sessionId=" + sessionId + ']', t);

                        if (sesCtx != null)
                            sesCtx.session.onException(t);
                    }
                    finally {
                        U.closeQuiet(channel);
                    }
                }
            });
        }
        else
            U.warn(log, "The topic already have an appropriate channel handler factory [topic=" + topic + ']');
    }

    /**
     * @param topic The topic to erase handler from.
     */
    public void removeFileIoChannelHandler(Object topic) {
        assert busyLock.blockedByCurrentThread();

        topicFactoryMap.remove(topic);

        ctx.io().removeChannelListener(topic);
    }

    /**
     * @param sessionId The session identifier to get context.
     * @return The current session chunked stream of {@code null} if session not found.
     */
    ChunkedStream sessionChunkedStream(String sessionId) {
        return sessionContextMap.get(sessionId) == null ? null : sessionContextMap.get(sessionId).stream;
    }

    /**
     * @param sessionId The session identifier to get context.
     * @return The current session channel which is processing.
     */
    TransmitInputChannel sessionInputChannel(String sessionId) {
        return sessionContextMap.get(sessionId) == null ? null : sessionContextMap.get(sessionId).currInput;
    }

    /**
     * @param factory A new factory instance to set.
     */
    void chunkedStreamFactory(ChunkedStreamFactory factory) {
        streamFactory = factory;
    }

    /**
     * @param sesCtx The handler read context.
     */
    private void onChannelCreated0(String sessionId, FileIoReadContext sesCtx) {
        ChunkedInputStream inStream = null;
        TransmitMeta meta = null;
        boolean stopped = false;

        try {
            while (!Thread.currentThread().isInterrupted() && !stopped) {
                if (!busyLock.enterBusy())
                    throw new NodeStoppingException("The node is stopping. Download session cannot be finished " +
                        "[sessionId=" + sessionId + ']');

                try {
                    // Read current stream session policy
                    ReadPolicy currPlc = sesCtx.currInput.readPolicy();

                    if (currPlc == ReadPolicy.NONE) {
                        sesCtx.session.end();
                        sessionContextMap.remove(sessionId);

                        break;
                    }

                    long startTime = U.currentTimeMillis();

                    sesCtx.currPlc = currPlc;

                    if (sesCtx.stream == null)
                        sesCtx.stream = streamFactory.createInputStream(sesCtx.currPlc, sesCtx.session, ioStreamChunkSize);

                    inStream = sesCtx.stream;
                    inStream.setup(sesCtx.currInput);

                    // Read data from the input.
                    while (!inStream.endStream()) {
                        if (Thread.currentThread().isInterrupted())
                            throw new InterruptedException("The thread has been interrupted. Stop processing input stream.");

                        // If the limit of permits at appropriate period of time reached,
                        // the furhter invocations of the #acuqire(int) method will be blocked.
                        permitsSemaphore.acquire(sesCtx.stream.chunkSize());

                        inStream.readChunk(sesCtx.currInput);
                    }

                    inStream.close();

                    sesCtx.stream = null;

                    // Write stream processing acknowledge.
                    sesCtx.currOutput.acknowledge(inStream.count());

                    long downloadTime = U.currentTimeMillis() - startTime;

                    U.log(log, "The file has been successfully downloaded " +
                        "[name=" + inStream.name() + ", transferred=" + ByteUnit.BYTE.toKB(inStream.transferred()) + " Kb" +
                        ", time=" + (double)((downloadTime) / 1000) + " sec" +
                        ", rate=" + ByteUnit.BYTE.toKB(permitsSemaphore.permitsPerSec()) + " Kb/sec" +
                        ", reconnects=" + sesCtx.reconnects);
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        }
        catch (RemoteTransmitException e) {
            // Waiting for re-establishing connection.
            log.warning("The connection lost. Waiting for the new one to continue file upload", e);

            sesCtx.reconnects++;

            if (sesCtx.reconnects == reconnectCnt) {
                IOException ex = new IOException("The number of reconnect attempts exceeded the limit. " +
                    "Max attempts: " + reconnectCnt, e);

                sesCtx.session.onException(ex);
            }
        }
        catch (Throwable t) {
            log.error("The download session cannot be finished due to unexpected error [ctx=" + sesCtx +
                ", channel=" + sesCtx.currInput + ", lastMeta=" + meta + ']', t);

            sesCtx.session.onException(t);
        }
        finally {
            U.closeQuiet(inStream);
        }
    }

    /**
     * @param remoteId The remote note to connect to.
     * @param topic The remote topic to connect to.
     * @param plc The remote prcessing channel policy.
     * @return The channel instance to communicate with remote.
     */
    public FileWriter fileWriter(
        UUID remoteId,
        Object topic,
        byte plc
    ) {
        return new FileWriterImpl(remoteId, topic, plc);
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
        private final byte plc;

        /** */
        private String sessionId;

        /** */
        private TransmitOutputChannel out;

        /** */
        private TransmitInputChannel in;

        /**
         * @param remoteId The remote note to connect to.
         * @param topic The remote topic to connect to.
         * @param plc The remote prcessing channel policy.
         */
        public FileWriterImpl(
            UUID remoteId,
            Object topic,
            byte plc
        ) {
            this.remoteId = remoteId;
            this.topic = topic;
            this.plc = plc;
            this.sessionId = UUID.randomUUID().toString();
        }

        /**
         * @throws IgniteCheckedException If fails.
         */
        public void connect() throws IgniteCheckedException {
            try {
                IgniteSocketChannel igCh = ctx.io().channelToTopic(remoteId, topic, plc,
                    Collections.singletonMap(SESSION_ID_KEY, sessionId));

                out = new TransmitOutputChannel(ctx, igCh);
                in = new TransmitInputChannel(ctx, igCh);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Error sending initial session meta to remote [remoteId=" + remoteId +
                    ", topic=" + topic + ", plc=" + plc + ']', e);
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
                        long position,
                        long count,
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

                    if (!busyLock.enterBusy())
                        throw new NodeStoppingException("The node is stopping. Upload session cannot be finished " +
                            "[sessionId=" + sessionId + ']');

                    try {
                        if (out == null)
                            connect();

                        String lastFileName = out.igniteChannel().attr(LAST_FILE_NAME_KEY);
                        Long receivedBytes = out.igniteChannel().attr(RECEIVED_BYTES_KEY);

                        assert lastFileName == null || fileStream.name().equals(lastFileName) :
                            "Attempt to upload different file [local=" + fileStream.name() + ", remote=" + lastFileName + ']';
                        assert receivedBytes == null || receivedBytes >= 0;

                        fileStream.transferred(ofNullable(receivedBytes).orElse(0L));

                        // Write the policy how to handle input data.
                        out.writePolicy(plc);

                        fileStream.setup(out);

                        while (!fileStream.endStream()) {
                            if (Thread.currentThread().isInterrupted())
                                throw new InterruptedException("The thread has been interrupted. Stop uploading file.");

                            fileStream.writeChunk(out);
                        }

                        fileStream.close();

                        in.acknowledge();

                        break;
                    }
                    catch (IOException | IgniteCheckedException e) {
                        closeChannelQuiet();

                        // Re-establish the new connection to continue upload.
                        U.warn(log, "Exception while writing file to remote node. Re-establishing connection " +
                            " [remoteId=" + remoteId + ", file=" + file.getName() + ", sessionId=" + sessionId +
                            ", reconnects=" + reconnects + ", transferred=" + fileStream.transferred() +
                            ", count=" + fileStream.count() + ']', e);

                        reconnects++;
                    }
                    finally {
                        busyLock.leaveBusy();
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
                    "[remoteId=" + remoteId + ", file=" + file.getName() + ", sessionId=" + sessionId + ']', e);
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

            out = null;
        }
    }

    /**
     *
     */
    private static class FileIoReadContext {
        /** The remote node input channel came from. */
        private final UUID nodeId;

        /** Current sesssion. */
        @GridToStringExclude
        private final TransmitSession session;

        /** Flag indicates session started. */
        private final AtomicBoolean started = new AtomicBoolean();

        /** The number of reconnect attempts of current session. */
        private int reconnects;

        /** The currently used input channel (updated on reconnect). */
        @GridToStringExclude
        private TransmitInputChannel currInput;

        /** The currently used output channel (updated on reconnect). */
        @GridToStringExclude
        private TransmitOutputChannel currOutput;

        /** The read policy of handlind input data. */
        private ReadPolicy currPlc;

        /** The last infinished download. */
        private ChunkedInputStream stream;

        /**
         * @param nodeId The remote node id.
         * @param session The channel handler.
         */
        public FileIoReadContext(UUID nodeId, TransmitSession session) {
            this.nodeId = nodeId;
            this.session = session;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileIoReadContext.class, this);
        }
    }
}
