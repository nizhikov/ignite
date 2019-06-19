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
import org.apache.ignite.internal.managers.communication.transmit.channel.InputTransmitChannel;
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

        this.handler = Objects.requireNonNull(handler);
    }

    /**
     * @param handler The chunk handler to process each chunk.
     */
    public InputChunkedBuffer(ChunkHandler handler) {
        this(handler, null, -1, -1, null);
    }

    /** {@inheritDoc} */
    @Override protected void init(int chunkSize) throws IOException {
        if (buff == null) {
            int buffSize = handler.begin(name(), params());

            int size = buffSize > 0 ? buffSize : chunkSize;

            chunkSize(size);

            buff = ByteBuffer.allocate(size);
            buff.order(ByteOrder.nativeOrder());
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
            throw new TransmitException("Socket has been unexpectedly closed, but stream is not fully processed");
        else
            // readed == 0
            return;

        buff.flip();

        boolean accepted = handler.chunk(buff, startPosition() + chunkPos);

        if (!accepted)
            throw new IOException("The buffer was rejected by handler");

        if (transferred.get() == cnt)
            handler.end(params());

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
