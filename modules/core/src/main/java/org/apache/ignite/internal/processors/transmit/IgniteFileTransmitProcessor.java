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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.transmit.channel.RemoteTransmitException;
import org.apache.ignite.internal.processors.transmit.channel.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.channel.TransmitMeta;
import org.apache.ignite.internal.processors.transmit.channel.TransmitOutputChannel;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedBufferStream;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedFileStream;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedStream;
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
    /** Reconnect attempts count to send single file. */
    private static final int DFLT_RECONNECT_CNT = 5;

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

    /** */
    private DiscoveryEventListener discoLsnr;

    /** The number of reconnects of current trasmission process (read or write attempts). */
    private volatile int reconnectCnt = DFLT_RECONNECT_CNT;

    /** */
    private final Object mux = new Object();

    /**
     * @param ctx Kernal context.
     */
    public IgniteFileTransmitProcessor(GridKernalContext ctx) {
        super(ctx);
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

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addDiscoveryEventListener(discoLsnr = (evt, disco) -> {
            UUID leftNodeId = evt.eventNode().id();

            // Clear the context on the uploader node left.
            for (Map.Entry<String, FileIoReadContext> sesEntry : sessionContextMap.entrySet()) {
                FileIoReadContext ioctx = sesEntry.getValue();

                if (ioctx.nodeId.equals(leftNodeId)) {
                    ClusterTopologyCheckedException ex;

                    ioctx.sesHndlr.onException(ex = new ClusterTopologyCheckedException("Failed to proceed download. " +
                        "The remote node node left the grid: " + leftNodeId));

                    sessionContextMap.remove(sesEntry.getKey());
                }
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        ctx.event().removeDiscoveryEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        synchronized (mux) {
            for (Object topic : topicFactoryMap.keySet())
                remoteFileIoChannelHandler(topic);
        }
    }

    /**
     * @param topic The {@link GridTopic} to register handler to.
     * @param factory The factory will create a new handler for each created channel.
     */
    public void addFileIoChannelHandler(Object topic, TransmitSessionFactory factory) {
        synchronized (mux) {
            if (topicFactoryMap.putIfAbsent(topic, factory) == null) {
                ctx.io().addChannelListener(topic, new GridIoChannelListener() {
                    @Override public Map<String, Serializable> onChannelConfigure(IgniteSocketChannel channel) {
                        // Restore the last received session state on remote node.
                        String sessionId = channel.attr(SESSION_ID_KEY);

                        assert sessionId != null;

                        FileIoReadContext readCtx = sessionContextMap.get(sessionId);

                        if (readCtx == null)
                            return null;
                        else {
                            ChunkedStream stream = readCtx.currIo;

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

                    @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                        String sessionId = null;

                        try {
                            // A new channel established, read the transfer session id first.
                            final TransmitInputChannel inChannel = new TransmitInputChannel(ctx, channel);

                            sessionId = Objects.requireNonNull(channel.attr(SESSION_ID_KEY));

                            final FileIoReadContext readCtx = sessionContextMap.computeIfAbsent(sessionId,
                                ses -> {
                                    final TransmitSession sesHndlr = factory.create();

                                    sesHndlr.begin(nodeId, ses);

                                    return new FileIoReadContext(nodeId, sesHndlr, reconnectCnt);
                                });

                            readCtx.currInputCh = inChannel;

                            onChannelCreated0(readCtx);
                        } catch (Exception e) {
                            log.error("Error processing channel creation event [topic=" + topic +
                                ", channel=" + channel + ", sessionId=" + sessionId + ']', e);
                        }
                        finally {
                            U.closeQuiet(channel);
                        }
                    }
                });
            }
            else
                U.warn(log,"The topic already have an appropriate channel handler factory [topic=" + topic + ']');
        }
    }

    /**
     * @param topic The topic to erase handler from.
     */
    public void remoteFileIoChannelHandler(Object topic) {
        synchronized (mux) {
            topicFactoryMap.remove(topic);

            ctx.io().removeChannelListener(topic);
        }
    }

    /**
     * @param sessionId The session identifier to get context.
     * @return The current session chunked stream of {@code null} if session not found.
     */
    ChunkedStream sessionChunkedStream(String sessionId) {
        return sessionContextMap.get(sessionId) == null ? null : sessionContextMap.get(sessionId).currIo;
    }

    /**
     * @param sessionId The session identifier to get context.
     * @return The current session channel which is processing.
     */
    TransmitInputChannel sessionInputChannel(String sessionId) {
        return sessionContextMap.get(sessionId) == null ? null : sessionContextMap.get(sessionId).currInputCh;
    }

    /**
     * @param readCtx The handler read context.
     */
    private void onChannelCreated0(FileIoReadContext readCtx) {
        ChunkedStream inChunkStream = null;

        try {
            TransmitMeta meta;
            boolean stopped = false;

            while (!Thread.currentThread().isInterrupted() && !stopped) {
                readCtx.currInputCh.readMeta(meta = new TransmitMeta());

                if (meta.equals(TransmitMeta.tombstone())) {
                    readCtx.sesHndlr.end();

                    break;
                }

                readCtx.currPlc = meta.policy();

                // Loading the file the first time.
                if (meta.initial()) {
                    if (readCtx.currIo != null) {
                        throw new IgniteCheckedException("Receive the offer to download a new file which was " +
                            "previously not been fully loaded [file=" + meta.name() +
                            ", unfinished=" + readCtx.currIo.name() + ']');
                    }

                    switch (readCtx.currPlc) {
                        case FILE:
                            inChunkStream = new ChunkedFileStream(readCtx.sesHndlr.fileHandler(), meta.name(), meta.offset(),
                                meta.count(), meta.params());

                            break;

                        case BUFF:
                            inChunkStream = new ChunkedBufferStream(readCtx.sesHndlr.chunkHandler(), meta.name(),
                                meta.offset(), meta.count(), meta.params());

                            break;

                        default:
                            throw new IgniteCheckedException("The type of read policy is unknown. The impelentation " +
                                "required: " + readCtx.currPlc);
                    }

                    readCtx.currIo = inChunkStream;
                }
                else {
                    inChunkStream = readCtx.currIo;

                    assert inChunkStream != null : "The file stream must be previously initialized [meta=" + meta +']';
                    assert meta.policy() == readCtx.currPlc :
                        "Attempt to process the same input with the different read policy: " + meta.policy();
                    assert inChunkStream.name().equals(meta.name()) : "Attempt to load different file name " +
                        "[name=" + inChunkStream.name() + ", meta=" + meta + ']';
                    assert inChunkStream.startPosition() + inChunkStream.transferred() == meta.offset() :
                        "The next segmented input is incorrect [postition=" + inChunkStream.startPosition() +
                            ", transferred=" + inChunkStream.transferred() + ", meta=" + meta + ']';
                    assert inChunkStream.count() - inChunkStream.transferred() == meta.count() :
                        " The count of bytes to transfer fot the next segment is incorrect " +
                            "[size=" + inChunkStream.count() + ", transferred=" + inChunkStream.transferred() +
                            ", meta=" + meta + ']';
                }

                inChunkStream.init();

                // Read data from the input.
                while (!inChunkStream.endOfStream()) {
                    if (Thread.currentThread().isInterrupted())
                        throw new InterruptedException("The thread has been interrupted. Stop processing input stream.");

                    inChunkStream.readChunk(readCtx.currInputCh);
                }

                inChunkStream.close();
                readCtx.currIo = null;
            }
        }
        catch (RemoteTransmitException e) {
            // Waiting for re-establishing connection.
            log.warning("The connection lost. Waiting for the new one to continue load", e);

            readCtx.reconnectCnt--;

            if (readCtx.reconnectCnt == 0) {
                IOException ex = new IOException("The number of reconnect attempts exceeded the limit. " +
                    "Max attempts: " + reconnectCnt);

                readCtx.sesHndlr.onException(ex);
            }
        }
        catch (Throwable t) {
            readCtx.sesHndlr.onException(t);

            log.error("The download session cannot be finished due to unexpected error [ctx=" + readCtx +
                ", channel=" + readCtx.currInputCh + ']', t);
        }
        finally {
            U.closeQuiet(inChunkStream);
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
        private TransmitOutputChannel ch;

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
                IgniteSocketChannel sock = ctx.io().channelToTopic(remoteId, topic, plc,
                    Collections.singletonMap(SESSION_ID_KEY, sessionId));

                ch = new TransmitOutputChannel(ctx, sock);
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

            ChunkedFileStream outChunkStream = new ChunkedFileStream(
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
                params);

            try {
                while (true) {
                    if (Thread.currentThread().isInterrupted())
                        throw new InterruptedException("The thread has been interrupted. Stop uploading file.");

                    if (reconnects > reconnectCnt)
                        throw new IOException("The number of reconnect attempts exceeded the limit: " + reconnectCnt);

                    try {
                        if (ch == null)
                            connect();

                        String lastFileName = ch.igniteChannel().attr(LAST_FILE_NAME_KEY);
                        Long receivedBytes = ch.igniteChannel().attr(RECEIVED_BYTES_KEY);

                        assert lastFileName == null || outChunkStream.name().equals(lastFileName) :
                            "Attempt to upload different file [local=" + outChunkStream.name() + ", remote=" + lastFileName + ']';
                        assert receivedBytes == null || receivedBytes >= 0;

                        outChunkStream.transferred(ofNullable(receivedBytes).orElse(0L));

                        ch.writeMeta(new TransmitMeta(outChunkStream.name(),
                            outChunkStream.startPosition() + outChunkStream.transferred(),
                            outChunkStream.count(),
                            outChunkStream.transferred() == 0,
                            plc,
                            outChunkStream.params()));

                        outChunkStream.init();

                        while (!outChunkStream.endOfStream()) {
                            if (Thread.currentThread().isInterrupted())
                                throw new InterruptedException("The thread has been interrupted. Stop uploading file.");

                            outChunkStream.writeChunk(ch);
                        }

                        outChunkStream.close();

                        break;
                    }
                    catch (IOException | IgniteCheckedException e) {
                        closeChannelQuiet();

                        reconnects++;

                        // Re-establish the new connection to continue upload.
                        U.warn(log, "An exception occured during file transmission. Re-establishing connection " +
                            " [remoteId=" + remoteId + ", file=" + file.getName() + ", sessionId=" + sessionId +
                            ", reconnects=" + reconnects + ", transferred=" + outChunkStream.transferred() +
                            ", count=" + outChunkStream.count() + ']', e);
                    }
                }
            }
            catch (Exception e) {
                closeChannelQuiet();

                throw new IgniteCheckedException("Exception while uploading file to the remote node. The process stopped " +
                    "[remoteId=" + remoteId + ", file=" + file.getName() + ", sessionId=" + sessionId + ']', e);
            }
            finally {
                U.closeQuiet(outChunkStream);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            try {
                if (ch != null) {
                    U.log(log, "Writing tombstone on close write session");

                    ch.writeMeta(TransmitMeta.tombstone());
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
            U.closeQuiet(ch);

            ch = null;
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
        private final TransmitSession sesHndlr;

        /** The number of reconnect attempts of current session. */
        private int reconnectCnt;

        /** The currently used input channel. */
        @GridToStringExclude
        private TransmitInputChannel currInputCh;

        /** The read policy of handlind input data. */
        private ReadPolicy currPlc;

        /** The last infinished download. */
        private ChunkedStream currIo;

        /**
         * @param nodeId The remote node id.
         * @param sesHndlr The channel handler.
         * @param reconnectCnt The number of reconnect attempts.
         */
        public FileIoReadContext(UUID nodeId, TransmitSession sesHndlr, int reconnectCnt) {
            this.nodeId = nodeId;
            this.sesHndlr = sesHndlr;
            this.reconnectCnt = reconnectCnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileIoReadContext.class, this);
        }
    }
}
