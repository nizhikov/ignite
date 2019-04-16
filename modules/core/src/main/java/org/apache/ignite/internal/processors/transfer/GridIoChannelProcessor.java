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
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
public class GridIoChannelProcessor extends GridProcessorAdapter {
    /** */
    private final ConcurrentMap<Object, FileIoReadFactory> handlers = new ConcurrentHashMap<>();

    /** */
    private final Object mux = new Object();

    /**
     * @param ctx Kernal context.
     */
    protected GridIoChannelProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param topic The {@link GridTopic} to register handler to.
     * @param factory The factory will create a new handler for each created channel.
     */
    public void addFileIoChannelHandler(Object topic, FileIoReadFactory factory) {
        synchronized (mux) {
            if (handlers.putIfAbsent(topic, factory) == null) {
                ctx.io().addChannelListener(topic, new GridIoChannelListener() {
                    @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                        onChannelCreated0(nodeId, channel, factory);
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
            handlers.remove(topic);
        }
    }

    /**
     * @param nodeId
     * @param channel
     * @param factory
     */
    private void onChannelCreated0(UUID nodeId, IgniteSocketChannel channel, FileIoReadFactory factory) {
        try {
            final FileIoReadHandler hndlr = factory.create();
            final FileIoChannel ch = new FileIoChannel(ctx, channel, new GridFutureAdapter<>());

            hndlr.created(nodeId, channel);

            IoMeta lastMeta;

            for (;;) {
                ch.doReadMeta(lastMeta = new IoMeta());

                if (lastMeta.equals(IoMeta.tombstone()))
                    break;

                Object destObj = hndlr.accept(lastMeta);

                if (destObj instanceof ByteBuffer) {
                    ByteBuffer buff = (ByteBuffer) destObj;
                    long position = lastMeta.offset();
                    long count = lastMeta.count();

                    long readed;
                    while (position < count) {
                        readed = ch.doReadRaw(buff);

                        if (readed < 0)
                            break;

                        position += readed;

                        hndlr.accept(buff);
                    }
                }
                else if (destObj instanceof File) {
                    File file = (File) destObj;

                    ch.doRead(new FileSegment(file,
                            lastMeta.name(),
                            lastMeta.offset(),
                            lastMeta.count()),
                        lastMeta.offset(),
                        lastMeta.count());
                }
                else
                    throw new UnsupportedOperationException("The destination object is unknown type: " + destObj.getClass());
            }

        } catch (Exception e) {
            log.error("Error handling channel [nodeId=" + nodeId + ", channel=" + channel + ']');
        }
    }

    /**
     * @param remoteId The remote note to connect to.
     * @param topic The remote topic to connect to.
     * @param plc The remote prcessing channel policy.
     * @param fut The control channel future.
     * @param <T> The type of transfer file meta info to send.
     * @return The channel instance to communicate with remote.
     */
    public <T extends IoMeta> FileIoChannelWriter writableChannel(
        UUID remoteId,
        Object topic,
        byte plc,
        IgniteInternalFuture<?> fut
    ) throws IgniteCheckedException {
        return new FileIoChannelWriterImpl(remoteId, topic, plc, fut)
            .connect();
    }

    /**
     *
     */
    private class FileIoChannelWriterImpl implements FileIoChannelWriter {
        /** */
        private final UUID remoteId;

        /** */
        private final Object topic;

        /** */
        private final byte plc;

        /** */
        private final IgniteInternalFuture<?> fut;

        /** */
        private FileIoChannel ch;

        /**
         * @param remoteId The remote note to connect to.
         * @param topic The remote topic to connect to.
         * @param plc The remote prcessing channel policy.
         */
        public FileIoChannelWriterImpl(
            UUID remoteId,
            Object topic,
            byte plc,
            IgniteInternalFuture<?> fut
        ) {
            this.remoteId = remoteId;
            this.topic = topic;
            this.plc = plc;
            this.fut = fut;
        }

        /**
         * @return The current initialized channel instance.
         * @throws IgniteCheckedException If fails.
         */
        public FileIoChannelWriter connect() throws IgniteCheckedException {
            ch = new FileIoChannel(ctx, ctx.io().channelToTopic(remoteId, topic, plc), fut);

            return this;
        }

        /** {@inheritDoc} */
        @Override public void write(File file, IoMeta meta, long offset, long count) throws IgniteCheckedException {
            // TODO reconnect if need.
            ch.doWrite(meta, new FileSegment(file, file.getName(), offset, count));
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            U.closeQuiet(ch);
        }
    }

    /**
     *
     */
    private static class FileIoHandlerContext {
        /** */
        private final FileIoReadHandler hndlr;

        /** */
        private final FileIoChannel ioCh;

        /** */
        private IoMeta lastMeta;

        /**
         * @param hndlr The handler to of appropriate channel.
         * @param ioCh The channel to read data from.
         */
        public FileIoHandlerContext(FileIoReadHandler hndlr, FileIoChannel ioCh) {
            this.hndlr = hndlr;
            this.ioCh = ioCh;
        }

        /**
         * @throws IgniteCheckedException If fails.
         */
        public void readMeta() throws IgniteCheckedException {
            ioCh.doReadMeta(lastMeta);
        }
    }
}
