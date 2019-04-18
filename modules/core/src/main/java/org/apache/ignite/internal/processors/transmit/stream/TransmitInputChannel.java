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

package org.apache.ignite.internal.processors.transmit.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
public class TransmitInputChannel extends TransmitAbstractChannel {
    /** */
    private final ObjectInput dis;

    /** If the channel is not been used anymore. */
    private AtomicBoolean stopped = new AtomicBoolean();

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     * @throws IOException If fails.
     */
    public TransmitInputChannel(
        GridKernalContext ktx,
        IgniteSocketChannel channel
    ) throws IOException {
        super(ktx, channel);

        this.dis = new ObjectInputStream(this.channel.socket().getInputStream());

    }

    /**
     * @param meta The meta to read to.
     * @throws IOException If fails.
     */
    public void readMeta(TransmitMeta meta) throws IOException {
        try {
            if (stopped.get())
                throw new IOException("Channel is stopped. Reading meta is not allowed.");

            meta.readExternal(dis);

            if (log.isDebugEnabled())
                log.debug("The file meta info have been received [meta=" + meta + ']');
        }
        catch (EOFException e) {
            throw new IOException("Input connection closed unexpectedly", e);
        }
        catch (ClassNotFoundException e) {
            throw new IOException("Read file meta error", e);
        }
    }

    /**
     * @param fileIO The I\O file
     * @param position The position to start from.
     * @param count The number of bytes to read.
     * @return The number of readed bytes.
     * @throws IOException If fails.
     */
    public long readInto(FileIO fileIO, long position, long count) throws IOException {
        if (stopped.get())
            return -1;

        return fileIO.transferFrom((ReadableByteChannel)channel, position, count);
    }

    /**
     * @param buff Buffer to read data into.
     * @return The number of bytes read, possibly zero, or <tt>-1</tt> if the channel has reached end-of-stream.
     * @throws IOException If fails.
     */
    public long readInto(ByteBuffer buff) throws IOException {
        if (stopped.get())
            return -1;

        return channel.read(buff);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        super.close();

        U.closeQuiet(dis);
    }
}
