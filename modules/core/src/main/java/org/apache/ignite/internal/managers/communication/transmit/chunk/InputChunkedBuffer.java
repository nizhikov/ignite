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
     * @param handler The chunk handler to process each chunk.
     */
    public InputChunkedBuffer(ChunkHandler handler) {
        this(handler, null, -1, -1, null);
    }

    /** {@inheritDoc} */
    @Override protected void init(int chunkSize) throws IOException {
        if (buff != null)
            return;

        int buffSize = handler.begin(name(), params());

        int size = buffSize > 0 ? buffSize : chunkSize;

        chunkSize(size);

        buff = ByteBuffer.allocate(size);
        buff.order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException {
        buff.rewind();

        long readed = tryReadFully(ch, buff);

        long prevReaded = transferred();

        if (readed > 0)
            transferred += readed;
        else
            return;

        buff.flip();

        handler.chunk(buff);
    }

    /** {@inheritDoc} */
    @Override public void doRead(ReadableByteChannel ch) throws IOException, IgniteCheckedException {
        super.doRead(ch);

        if (transferred() == count())
            handler.end(params());

        checkTransferLimitCount();
    }

    /**
     * Reads data from input channel utill the given buffer will be completely
     * filled ({@code buff.remaining()} returns 0) or partitially filled buffer if it was the last chunk.
     *
     * @param ch Channel to read data from.
     * @param buff Buffer to write data to.
     * @return Number of bytes actually readed into given buffer.
     * @throws TransmitException if the object has not been fully loaded while the stream reached its end.
     */
    private int tryReadFully(ReadableByteChannel ch, ByteBuffer buff) throws IOException {
        int total = 0;

        while (true) {
            int readed = ch.read(buff);

            if (readed < 0) {
                // Return partitially filled buffer only for the last chunk
                if (transferred + total == count())
                    return total;
                else
                    throw new TransmitException("Input data channel reached its end, but chunked object has not fully loaded");
            }

            total += readed;

            if (total == buff.capacity() || buff.position() == buff.capacity())
                return total;
        }
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
