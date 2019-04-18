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

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
class FileIoChannel implements AutoCloseable {
    /** */
    private final IgniteLogger log;

    /** */
    private final SocketChannel channel;

    /** */
    private final ObjectInput dis;

    /** */
    private final ObjectOutput dos;

    /** If the channel is not been used anymore. */
    private AtomicBoolean stopped = new AtomicBoolean();

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     * @throws IOException If fails.
     */
    public FileIoChannel(
        GridKernalContext ktx,
        IgniteSocketChannel channel
    ) throws IOException {
        assert channel.config().blocking();

        this.channel = channel.channel();
        this.dis = new ObjectInputStream(this.channel.socket().getInputStream());
        this.dos = new ObjectOutputStream(this.channel.socket().getOutputStream());
        this.log = ktx.log(getClass());
    }

    /**
     * @param stopped The flag to set to.
     */
    void stopped(AtomicBoolean stopped) {
        this.stopped = stopped;
    }

    /**
     * @param meta The file meta to write from.
     * @throws IOException If fails.
     */
    void writeMeta(ChannelIoMeta meta) throws IOException {
        if (stopped.get())
            throw new IOException("Channel is stopped. Writing meta is not allowed.");

        meta.writeExternal(dos);

        dos.flush();

        if (log.isDebugEnabled())
            log.debug("The file meta info have been written:" + meta + ']');

    }

    /**
     * @param meta The meta to read to.
     * @throws IOException If fails.
     */
    void readMeta(ChannelIoMeta meta) throws IOException {
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
    long readInto(FileIO fileIO, long position, long count) throws IOException {
        if (stopped.get())
            return -1;

        return fileIO.transferFrom((ReadableByteChannel)channel, position, count);
    }

    /**
     * @param position The position to start from.
     * @param count The number of bytes to write.
     * @param fileIO The I\O file
     * @return The number of writed bytes.
     * @throws IOException If fails.
     */
    long writeFrom(long position, long count, FileIO fileIO) throws IOException {
        if (stopped.get())
            return -1;

        return fileIO.transferTo(position, count, (WritableByteChannel)channel);
    }

    /**
     * @param buff Buffer to read data into.
     * @return The number of bytes read, possibly zero, or <tt>-1</tt> if the channel has reached end-of-stream.
     * @throws IOException If fails.
     */
    long readInto(ByteBuffer buff) throws IOException {
        if (stopped.get())
            return -1;

        return channel.read(buff);
    }

    /**
     * @param buff Buffer to write data from.
     * @return The number of bytes written, possibly zero, or <tt>-1</tt> if the channel has reached end-of-stream.
     * @throws IOException If fails.
     */
    long writeFrom(ByteBuffer buff) throws IOException {
        if (stopped.get())
            return -1;

        return channel.write(buff);
    }

    /**
     * @param file The file object to check.
     * @throws EOFException If the check fails.
     */
    public static void checkFileEOF(SegmentedFileIo file) throws EOFException {
        if (file.transferred() < file.count()) {
            throw new EOFException("The file expected to be fully transferred but didn't [count=" + file.count() +
                ", transferred=" + file.transferred() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(dos);
        U.closeQuiet(dis);
        U.closeQuiet(channel);
    }
}
