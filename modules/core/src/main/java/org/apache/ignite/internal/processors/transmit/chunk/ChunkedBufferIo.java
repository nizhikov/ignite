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
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.transmit.stream.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.stream.TransmitOutputChannel;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ChunkedBufferIo extends AbstractChunkedIo<ByteBuffer> {
    /**
     * @param buff The buff to read into.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     */
    public ChunkedBufferIo(ByteBuffer buff, String name, long position, long count) {
        super(buff, name, position, count);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer readFrom(TransmitInputChannel channel) throws IOException {
        long readed = channel.readInto(obj);

        if (readed > 0)
            transferred.add(readed);
        else if (readed < 0)
            checkChunkedIoEOF(this, channel);

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void writeInto(TransmitOutputChannel channel) throws IOException {
        long written = channel.writeFrom(obj);

        if (written > 0)
            transferred.add(written);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {

    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkedBufferIo.class, this);
    }
}
