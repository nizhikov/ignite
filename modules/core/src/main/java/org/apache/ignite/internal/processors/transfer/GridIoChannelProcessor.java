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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
public class GridIoChannelProcessor extends GridProcessorAdapter {
    /** */
    private final ConcurrentMap<Object, FileIoReadHandler<? extends IoMeta>> handlers = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    protected GridIoChannelProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param topic The {@link GridTopic} to register handler to.
     * @param hndlr The handler to process particular channel.
     * @param <T> The type of handled meta info.
     */
    public <T extends IoMeta> void addFileIoChannelHandler(Object topic, FileIoReadHandler<T> hndlr) {
        ctx.io().addChannelListener(topic, new GridIoChannelListener() {
            @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {

            }
        });

        handlers.putIfAbsent(topic, hndlr);
    }

    /**
     *
     */
    public <T extends IoMeta> void remoteFileIoChannelHandler(Object topic, FileIoReadHandler<T> hndlr) {
        handlers.remove(topic, hndlr);
    }

    /**
     * @param remoteId The remote note to connect to.
     * @param topic The remote topic to connect to.
     * @param plc The remote prcessing channel policy.
     * @param fut The control channel future.
     * @param <T> The type of transfer file meta info to send.
     * @return The channel instance to communicate with remote.
     */
    public <T extends IoMeta> FileIoChannelWriter<T> writableChannel(
        UUID remoteId,
        Object topic,
        byte plc,
        IgniteInternalFuture<?> fut
    ) throws IgniteCheckedException {
        return new FileIoChannelWriterImpl<T>(ctx, remoteId, topic, plc, fut)
            .connect();
    }

    /**
     *
     */
    private static class FileIoChannelWriterImpl<T extends IoMeta> implements FileIoChannelWriter<T> {
        /** */
        private final GridKernalContext ctx;

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
         * @param ctx Grid kernal context.
         * @param remoteId The remote note to connect to.
         * @param topic The remote topic to connect to.
         * @param plc The remote prcessing channel policy.
         */
        public FileIoChannelWriterImpl(
            GridKernalContext ctx,
            UUID remoteId,
            Object topic,
            byte plc,
            IgniteInternalFuture<?> fut
        ) {
            this.ctx = ctx;
            this.remoteId = remoteId;
            this.topic = topic;
            this.plc = plc;
            this.fut = fut;
        }

        /**
         * @return The current initialized channel instance.
         * @throws IgniteCheckedException If fails.
         */
        public FileIoChannelWriter<T> connect() throws IgniteCheckedException {
            ch = new FileIoChannel(ctx, ctx.io().channelToTopic(remoteId, topic, plc), fut);

            return this;
        }

        /** {@inheritDoc} */
        @Override public void doWrite(
            File file,
            T meta,
            long offset,
            long count
        ) throws IgniteCheckedException {
            // TODO reconnect if need.
            ch.doWrite(meta, new FileSegment(file, file.getName(), offset, count));
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            U.closeQuiet(ch);
        }
    }
}
