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

package org.apache.ignite.internal.managers.communication.transmit.chunk;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.managers.communication.transmit.ChunkHandler;
import org.apache.ignite.internal.managers.communication.transmit.channel.RemoteTransmitException;
import org.apache.ignite.internal.managers.communication.transmit.channel.InputTransmitChannel;
import org.apache.ignite.internal.managers.communication.transmit.channel.OutputTransmitChannel;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Buffered chunked stream to handle data input by chunks delivered to {@link ByteBuffer}.
 */
public class ChunkedBuffer extends AbstractChunkedObject {
    /** Chunked channel handler to process data with chunks. */
    private final ChunkHandler handler;

    /** The size of destination buffer. */
    private int buffSize;

    /** The destination object to transfer data to\from. */
    private ByteBuffer buff;

    /**
     * @param handler The chunk handler to process each chunk.
     * @param name The unique file name within transfer process.
     * @param pos The pos from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param chunkSize The size of chunk to read.
     * @param params Additional stream params.
     */
    public ChunkedBuffer(
        ChunkHandler handler,
        String name,
        long pos,
        long cnt,
        int chunkSize,
        Map<String, Serializable> params
    ) {
        super(name, pos, cnt, chunkSize, params);

        this.handler = Objects.requireNonNull(handler);
    }

    /**
     * @param handler The chunk handler to process each chunk.
     * @param chunkSize The size of chunk to read.
     */
    public ChunkedBuffer(ChunkHandler handler, int chunkSize) {
        this(handler, null, -1, -1, chunkSize, null);
    }

    /** {@inheritDoc} */
    @Override protected void init() throws IOException {
        if (buff == null) {
            buffSize = handler.begin(name(), params());

            assert buffSize > 0;

            buff = ByteBuffer.allocate(buffSize);
            buff.order(ByteOrder.nativeOrder());

            chunkSize(buffSize);
        }
    }

    /** {@inheritDoc} */
    @Override public void readChunk(InputTransmitChannel channel) throws IOException {
        buff.rewind();

        long readed = channel.read(buff);
        long chunkPos;

        if (readed > 0)
            chunkPos = transferred.getAndAdd(readed);
        else if (readed < 0 || transferred() < count())
            throw new RemoteTransmitException("Socket has been unexpectedly closed, but stream is not fully processed");
        else
            // readed == 0
            return;

        buff.flip();

        boolean accepted = handler.chunk(buff, startPosition() + chunkPos);

        if (!accepted)
            throw new IOException("The buffer was rejected by handler");

        if (transmitEnd())
            handler.end(params());
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        buff = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkedBuffer.class, this, "super", super.toString());
    }
}
