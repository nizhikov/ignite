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

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
public class TransmitOutputChannel extends TransmitAbstractChannel {
    /** */
    private final ObjectOutput dos;

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     * @throws IOException If fails.
     */
    public TransmitOutputChannel(
        GridKernalContext ktx,
        IgniteSocketChannel channel
    ) throws IOException {
        super(ktx, channel);

        this.dos = new ObjectOutputStream(this.channel.socket().getOutputStream());
    }

    /**
     * @param meta The file meta to write from.
     * @throws IOException If fails.
     */
    public void writeMeta(TransmitMeta meta) throws IOException {
        if (stopped.get())
            throw new IOException("Channel is stopped. Writing meta is not allowed.");

        meta.writeExternal(dos);

        dos.flush();

        if (log.isDebugEnabled())
            log.debug("The file meta info have been written:" + meta + ']');

    }

    /**
     * @param position The position to start from.
     * @param count The number of bytes to write.
     * @param fileIO The I\O file
     * @return The number of writed bytes.
     * @throws IOException If fails.
     */
    public long writeFrom(long position, long count, FileIO fileIO) throws IOException {
        if (stopped.get())
            return -1;

        return fileIO.transferTo(position, count, (WritableByteChannel)channel);
    }

    /**
     * @param buff Buffer to write data from.
     * @return The number of bytes written, possibly zero, or <tt>-1</tt> if the channel has reached end-of-stream.
     * @throws IOException If fails.
     */
    public long writeFrom(ByteBuffer buff) throws IOException {
        if (stopped.get())
            return -1;

        return channel.write(buff);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        super.close();

        U.closeQuiet(dos);
    }
}
