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

package org.apache.ignite.internal.processors.transfer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
public class GridFileTransmitProcessor extends GridProcessorAdapter {
    /** */
    private final ConcurrentMap<Object, FileReadHandlerFactory> topicFactoryMap = new ConcurrentHashMap<>();

    /** The map of already known channel read contexts by its session id. */
    private final ConcurrentMap<String, FileIoReadContext> sessionContextMap = new ConcurrentHashMap<>();

    /** */
    private final Object mux = new Object();

    /**
     * @param ctx Kernal context.
     */
    public GridFileTransmitProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param topic The {@link GridTopic} to register handler to.
     * @param factory The factory will create a new handler for each created channel.
     */
    public void addFileIoChannelHandler(Object topic, FileReadHandlerFactory factory) {
        synchronized (mux) {
            if (topicFactoryMap.putIfAbsent(topic, factory) == null) {
                ctx.io().addChannelListener(topic, new GridIoChannelListener() {
                    @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                        try {
                            // A new channel established, read the transfer session id first.
                            final FileIoChannel objChannel = new FileIoChannel(ctx, channel);

                            ChannelIoMeta sessionMeta;

                            objChannel.readMeta(sessionMeta = new ChannelIoMeta());

                            if (sessionMeta.equals(ChannelIoMeta.tombstone()))
                                return;

                            assert sessionMeta.initial();

                            onChannelCreated0(sessionContextMap.computeIfAbsent(sessionMeta.name(),
                                ses -> {
                                    final AtomicBoolean flag = new AtomicBoolean();
                                    final FileReadHandler hndlr = factory.create();

                                    hndlr.created(nodeId, ses, flag);

                                    return new FileIoReadContext(nodeId, ses, hndlr, flag);
                                }),
                                objChannel);
                        } catch (IOException e) {
                            log.error("Error processing channel creation event [topic=" + topic +
                                ", channel=" + channel + ']');
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
    private void onChannelCreated0(FileIoReadContext rctx, FileIoChannel chnl) {
        // Set the channel flag to stop.
        chnl.stopped(rctx.stopped);

        try {
            ChannelIoMeta meta;
            SegmentedIo<?> seg;

            while (!Thread.currentThread().isInterrupted() && !rctx.stopped.get()) {
                chnl.readMeta(meta = new ChannelIoMeta());

                if (meta.equals(ChannelIoMeta.tombstone())) {
                    rctx.stopped.set(true);

                    break;
                }

                // Loading the file the first time.
                if (meta.initial()) {
                    if (rctx.unfinished.containsKey(meta.name()))
                        throw new IgniteCheckedException("Receive the offer to download a new file which exists " +
                            "in `unfinished` session list [file=" + meta.name() + ", unfinished=" + rctx.unfinished + ']');

                    Object intoObj = rctx.handler.acceptFileMeta(meta.name(), meta.keys());

                    if (intoObj instanceof ByteBuffer)
                        seg = new SegmentedBufferIo((ByteBuffer)intoObj, meta.name(), meta.offset(), meta.count());
                    else if (intoObj instanceof File)
                        seg = new SegmentedFileIo((File)intoObj, meta.name(), meta.offset(), meta.count());
                    else
                        throw new IgniteCheckedException("The object to write to is unknown type: " + intoObj.getClass());

                    rctx.unfinished.put(meta.name(), seg);
                }
                else
                    seg = rctx.unfinished.get(meta.name());

                assert seg.postition() + seg.transferred() == meta.offset() :
                    "The next segmented input is incorrect [postition=" + seg.postition() +
                        ", transferred=" + seg.transferred() + ", offset=" + meta.offset() + ']';
                assert seg.count() - seg.transferred() == meta.count() :
                    " The count of bytes to transfer fot the next segment is incorrect [size=" + seg.count() +
                        ", transferred=" + seg.transferred() + ", count=" + meta.count() + ']';

                Object objReaded = null;

                // Read data from the input.
                while (!seg.endOfTransmit() && !rctx.stopped.get() && !Thread.currentThread().isInterrupted()) {
                    if (objReaded instanceof ByteBuffer)
                        rctx.handler.accept((ByteBuffer)objReaded);

                    objReaded = seg.readFrom(chnl);
                }

                if (objReaded instanceof File)
                    rctx.handler.accept((File)objReaded);
                else if (objReaded instanceof  ByteBuffer)
                    rctx.handler.accept((ByteBuffer)objReaded);
                else if (objReaded == null)
                    throw new IOException("The file has not been fully received: " + meta);
                else
                    throw new IgniteCheckedException("The destination object is unknown type: " + objReaded.getClass());
            }
        }
        catch (Exception e) {
            rctx.handler.exceptionCaught(e);

            log.error("Error handling download [ctx=" + rctx + ", channel=" + chnl + ']');
        }
        finally {
            U.closeQuiet(chnl);
        }
    }

    /**
     * @param remoteId The remote note to connect to.
     * @param topic The remote topic to connect to.
     * @param plc The remote prcessing channel policy.
     * @return The channel instance to communicate with remote.
     */
    public FileWriter writableChannel(
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
        private FileIoChannel ch;

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
                ch = new FileIoChannel(ctx, ctx.io().channelToTopic(remoteId, topic, plc));

                ch.writeMeta(new ChannelIoMeta(sessionId));

                return this;
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void write(File file, long offset, long count, Map<String, String> params) throws IgniteCheckedException {
            // TODO reconnect if need.
            try {
                ch.writeMeta(new ChannelIoMeta(file.getName(), offset, count, true, params));

                SegmentedFileIo segFile = new SegmentedFileIo(file, file.getName(), offset, count);

                while (!segFile.endOfTransmit() && !Thread.currentThread().isInterrupted())
                    segFile.writeInto(ch);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Error sending file to remote: " + file.getName(), e);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            try {
                ch.writeMeta(ChannelIoMeta.tombstone());
            }
            finally {
                U.closeQuiet(ch);
            }
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
        private final FileReadHandler handler;

        /** */
        private final AtomicBoolean stopped;

        /** The map of infinished downloads indexed by file name. */
        private final Map<String, SegmentedIo<?>> unfinished = new HashMap<>();

        /**
         * @param nodeId The remote node id.
         * @param sessionId The unique session id.
         * @param handler The channel handler.
         * @param stopped The stop flag to interrupt reads.
         */
        public FileIoReadContext(UUID nodeId, String sessionId, FileReadHandler handler, AtomicBoolean stopped) {
            this.nodeId = nodeId;
            this.sessionId = sessionId;
            this.handler = handler;
            this.stopped = stopped;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileIoReadContext.class, this);
        }
    }
}
