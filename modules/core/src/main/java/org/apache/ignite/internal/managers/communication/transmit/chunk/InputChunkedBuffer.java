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
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.transmit.ChunkHandler;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Buffered chunked object is handle input socket channel by chunks of data and
 * deliver it to an allocated {@link ByteBuffer}.
 */
public class InputChunkedBuffer extends InputChunkedObject {
    /** Chunked channel handler to process data with chunks. */
    private final ChunkHandler handler;

    /** The destination object to transfer data to\from. */
    private ByteBuffer buff;

    /**
     * @param handler The chunk handler to process each chunk.
     * @param name The unique file name within transfer process.
     * @param pos The pos from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     */
    public InputChunkedBuffer(
        ChunkHandler handler,
        String name,
        long pos,
        long cnt,
        Map<String, Serializable> params
    ) {
        super(name, pos, cnt, params);

        assert handler != null;

        this.handler = handler;
    }

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     * @param handler The chunk handler to process each chunk.
     */
    public InputChunkedBuffer(
        String name,
        long startPos,
        long cnt,
        Map<String, Serializable> params,
        ChunkHandler handler
    ) {
        this(handler, name, startPos, cnt, params);
    }

    /** {@inheritDoc} */
    @Override protected void init(int chunkSize) throws IOException {
        if (buff != null)
            return;

        int buffSize = handler.size();

        int size = buffSize > 0 ? buffSize : chunkSize;

        chunkSize(size);

        buff = ByteBuffer.allocate(size);
        buff.order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException {
        buff.rewind();

        int readed = 0;
        int res;

        // Read data from input channel utill the buffer will be completely filled
        // (buff.remaining() returns 0) or partitially filled buffer if it was the last chunk.
        while (true) {
            res = ch.read(buff);

            if (res < 0) {
                if (transferred + readed != count())
                    throw new TransmitException("Input data channel reached its end, but chunked object has not fully loaded");

                break;
            }

            readed += res;

            if (readed == buff.capacity() || buff.position() == buff.capacity())
                break;
        }

        if (readed == 0)
            return;

        transferred += readed;

        buff.flip();

        handler.accept(buff);
    }

    /** {@inheritDoc} */
    @Override public void doRead(ReadableByteChannel ch) throws IOException, IgniteCheckedException {
        super.doRead(ch);

        if (transferred() == count())
            handler.end();

        checkTransferLimitCount();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        buff = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InputChunkedBuffer.class, this, "super", super.toString());
    }
}
