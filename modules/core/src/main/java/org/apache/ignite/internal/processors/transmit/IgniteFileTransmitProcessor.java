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
import java.nio.ByteBuffer;
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
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedBufferIo;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedFileIo;
import org.apache.ignite.internal.processors.transmit.chunk.ChunkedIo;
import org.apache.ignite.internal.processors.transmit.stream.RemoteTransmitException;
import org.apache.ignite.internal.processors.transmit.stream.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.stream.TransmitMeta;
import org.apache.ignite.internal.processors.transmit.stream.TransmitOutputChannel;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 *
 */
public class IgniteFileTransmitProcessor extends GridProcessorAdapter {
    /** Reconnect attempts count to send single file. */
    private static final int DFLT_RECONNECT_CNT = 5;

    /** */
    private final ConcurrentMap<Object, TransmitSessionFactory> topicFactoryMap = new ConcurrentHashMap<>();

    /** The map of already known channel read contexts by its session id. */
    private final ConcurrentMap<String, FileIoReadContext> sessionContextMap = new ConcurrentHashMap<>();

    /** */
    private DiscoveryEventListener discoLsnr;

    /** */
    private final Object mux = new Object();

    /**
     * @param ctx Kernal context.
     */
    public IgniteFileTransmitProcessor(GridKernalContext ctx) {
        super(ctx);
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
                    @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                        try {
                            // A new channel established, read the transfer session id first.
                            final TransmitInputChannel objChannel = new TransmitInputChannel(ctx, channel);

                            TransmitMeta sessionMeta;

                            objChannel.readMeta(sessionMeta = new TransmitMeta());

                            if (sessionMeta.equals(TransmitMeta.tombstone()))
                                return;

                            assert sessionMeta.initial() : "The session meta message must be initialized";

                            onChannelCreated0(sessionContextMap.computeIfAbsent(sessionMeta.name(),
                                ses -> {
                                    final TransmitSession sesHndlr = factory.create();

                                    sesHndlr.begin(nodeId, ses);

                                    return new FileIoReadContext(nodeId, ses, sesHndlr);
                                }),
                                objChannel);
                        } catch (IOException e) {
                            log.error("Error processing channel creation event [topic=" + topic +
                                ", channel=" + channel + ']', e);
                        }
                        finally {
                            U.closeQuiet(channel);
                        }
                    }
                });
            }
            else
                log.warning("The topic already have an appropriate channel handler factory [topic=" + topic + ']');
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
     * @param rctx The handler read context.
     * @param chnl The connection channel instance.
     */
    private void onChannelCreated0(FileIoReadContext rctx, TransmitInputChannel chnl) {
        try {
            TransmitMeta meta;
            ChunkedIo<?> seg;
            boolean stopped = false;

            while (!Thread.currentThread().isInterrupted() && !stopped) {
                chnl.readMeta(meta = new TransmitMeta());

                if (meta.equals(TransmitMeta.tombstone())) {
                    rctx.sesHndlr.end();

                    break;
                }

                rctx.currPlc = meta.policy();
                rctx.currIoParams = meta.params();

                // Loading the file the first time.
                if (meta.initial()) {
                    if (rctx.currIo != null) {
                        throw new IgniteCheckedException("Receive the offer to download a new file which was " +
                            "previously not been fully loaded [file=" + meta.name() +
                            ", unfinished=" + rctx.currIo.name() + ']');
                    }

                    switch (rctx.currPlc) {
                        case FILE:
                            rctx.fileHndlr = Objects.requireNonNull(rctx.sesHndlr.fileHandler());

                            String absPath = rctx.fileHndlr.begin(meta.name(), meta.offset(), meta.count(), rctx.currIoParams);

                            seg = new ChunkedFileIo(new File(absPath), meta.name(), meta.offset(), meta.count());

                            break;

                        case BUFF:
                            rctx.chunkHndlr = Objects.requireNonNull(rctx.sesHndlr.chunkHandler());

                            int buffSize = rctx.chunkHndlr.begin(meta.name(), meta.offset(), meta.count(), rctx.currIoParams);

                            seg = new ChunkedBufferIo(ByteBuffer.allocate(buffSize), meta.name(), meta.offset(), meta.count());

                            break;

                        default:
                            throw new IgniteCheckedException("The type of read policy is unknown. The impelentation " +
                                "required: " + rctx.currPlc);
                    }

                    rctx.currIo = seg;
                }
                else {
                    seg = rctx.currIo;

                    assert meta.policy() == rctx.currPlc :
                        "Attempt to process the same input with the different read policy: " + meta.policy();
                    assert seg.name().equals(meta.name()) : "Attempt to load different file name [name=" + seg.name() +
                        ", meta=" + meta.name() + ']';
                    assert seg.postition() + seg.transferred() == meta.offset() :
                        "The next segmented input is incorrect [postition=" + seg.postition() +
                            ", transferred=" + seg.transferred() + ", offset=" + meta.offset() + ']';
                    assert seg.count() - seg.transferred() == meta.count() :
                        " The count of bytes to transfer fot the next segment is incorrect [size=" + seg.count() +
                            ", transferred=" + seg.transferred() + ", count=" + meta.count() + ']';
                }

                // Read data from the input.
                while (!seg.endOfTransmit() && !Thread.currentThread().isInterrupted()) {
                    seg.readChunk(chnl);

                    if (rctx.currPlc == ReadPolicy.BUFF)
                        rctx.chunkHndlr.chunk((ByteBuffer)seg.chunk());
                }

                switch (rctx.currPlc) {
                    case FILE:
                        rctx.fileHndlr.end((File)seg.chunk(), rctx.currIoParams);

                        break;

                    case BUFF:
                        rctx.chunkHndlr.end(rctx.currIoParams);

                        break;

                    default:
                        assert false : "The read policy is undefined: " + rctx.currPlc;
                }

                rctx.currIo = null;
            }
        }
        catch (RemoteTransmitException e) {
            // Waiting for re-establishing connection.
            log.warning("The connection lost. Waiting for the new one to continue load", e);

            rctx.reconnects--;

            if (rctx.reconnects == 0) {
                IOException ex = new IOException("The number of reconnect attempts exceeded the limit. " +
                    "Max attempts: " + DFLT_RECONNECT_CNT);

                rctx.sesHndlr.onException(ex);
            }
        }
        catch (Throwable t) {
            rctx.sesHndlr.onException(t);

            log.error("The download session cannot be finished due to unexpected error [ctx=" + rctx +
                ", channel=" + chnl + ']', t);
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
    ) throws IgniteCheckedException {
        return new FileWriterImpl(remoteId, topic, plc)
            .connect();
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
         * @return The current initialized channel instance.
         * @throws IgniteCheckedException If fails.
         */
        public FileWriter connect() throws IgniteCheckedException {
            try {
                IgniteSocketChannel sock = ctx.io().channelToTopic(remoteId, topic, plc);

                ch = new TransmitOutputChannel(ctx, sock);

                ch.writeMeta(new TransmitMeta(sessionId));

                return this;
            }
            catch (IOException e) {
                throw new IgniteCheckedException("The connection cannot be established [remoteId=" + remoteId +
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

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    if (reconnects > DFLT_RECONNECT_CNT)
                        throw new IOException("The number of reconnect attempts exceeded the limit: " + DFLT_RECONNECT_CNT);

                    if (ch == null)
                        connect();

                    try {
                        ch.writeMeta(new TransmitMeta(file.getName(), offset, count, true, plc, params));

                        ChunkedFileIo chunked = new ChunkedFileIo(file, file.getName(), offset, count);

                        while (!chunked.endOfTransmit() && !Thread.currentThread().isInterrupted())
                            chunked.writeChunk(ch);

                        break;
                    }
                    catch (RemoteTransmitException e) {
                        closeChannelQuiet();

                        reconnects++;

                        // Re-establish the new connection to continue upload.
                        U.warn(log, "An exception occured during file transmission. Re-establishing connection " +
                            " [remoteId=" + remoteId + ", file=" + file.getName() + ", sessionId=" + sessionId +
                            ", reconnects=" + reconnects + ']', e);
                    }
                }
            }
            catch (Exception e) {
                closeChannelQuiet();

                throw new IgniteCheckedException("Exception while uploading file to the remote node. The process stopped " +
                    "[remoteId=" + remoteId + ", file=" + file.getName() + ", sessionId=" + sessionId + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            try {
                if (ch != null)
                    ch.writeMeta(TransmitMeta.tombstone());
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
        /** */
        private final UUID nodeId;

        /** The unique session id. */
        private final String sessionId;

        /** */
        private final TransmitSession sesHndlr;

        /** */
        private ChunkHandler chunkHndlr;

        /** */
        private FileHandler fileHndlr;

        /** The number of reconnect attempts of current session. */
        private int reconnects = DFLT_RECONNECT_CNT;

        /** */
        private ReadPolicy currPlc;

        /** The last infinished download. */
        private ChunkedIo<?> currIo;

        /** */
        private Map<String, Serializable> currIoParams;

        /**
         * @param nodeId The remote node id.
         * @param sessionId The unique session id.
         * @param sesHndlr The channel handler.
         */
        public FileIoReadContext(UUID nodeId, String sessionId, TransmitSession sesHndlr) {
            this.nodeId = nodeId;
            this.sessionId = sessionId;
            this.sesHndlr = sesHndlr;
        }

        /**
         * @param params The input params to copy to context.
         */
        public void lastIoParams(Map<String, Serializable> params) {
            currIoParams = Collections.unmodifiableMap(new HashMap<>(params)); ;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileIoReadContext.class, this);
        }
    }
}
