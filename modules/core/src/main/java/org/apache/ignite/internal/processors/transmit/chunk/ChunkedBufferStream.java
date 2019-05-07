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

package org.apache.ignite.internal.processors.transmit.chunk;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.processors.transmit.ChunkHandler;
import org.apache.ignite.internal.processors.transmit.channel.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.channel.TransmitOutputChannel;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ChunkedBufferStream extends AbstractChunkedStream {
    /** */
    private final ChunkHandler hndlr;

    /** The size of destination buffer. */
    private int buffSize;

    /** The destination object to transfer data to\from. */
    private ByteBuffer buff;

    /**
     * @param handler The chunk handler to process each chunk.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     * @param chunkSize The size of chunk to read.
     * @param params Additional stream params.
     */
    public ChunkedBufferStream(
        ChunkHandler handler,
        String name,
        long position,
        long count,
        int chunkSize,
        Map<String, Serializable> params
    ) {
        super(name, position, count, chunkSize, params);

        this.buffSize = buffSize;
        this.hndlr = Objects.requireNonNull(handler);
    }

    /**
     * @throws IOException If initialization failed.
     */
    @Override public void init() throws IOException {
        if (buff == null) {
            buffSize = hndlr.begin(name(), startPosition(), count(), params());

            buff = ByteBuffer.allocate(buffSize);
        }
    }

    /** {@inheritDoc} */
    @Override public void readChunk(TransmitInputChannel channel) throws IOException {
        long readed = channel.readInto(buff);

        if (readed > 0)
            transferred.addAndGet(readed);
        else if (readed < 0)
            checkChunkedIoEOF(this);

        hndlr.chunk(buff);

        if (endOfStream())
            hndlr.end(params());
    }

    /** {@inheritDoc} */
    @Override public void writeChunk(TransmitOutputChannel channel) throws IOException {
        long written = channel.writeFrom(buff);

        if (written > 0)
            transferred.addAndGet(written);

        hndlr.chunk(buff);

        if (endOfStream())
            hndlr.end(params());
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        buff = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkedBufferStream.class, this, "super", super.toString());
    }
}
