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
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
public class FileIoChannel implements AutoCloseable {
    /** The message indicates the remote node finishes its transfer. */
    private static final byte TOMB_STONE = 13;

    /** */
    private final IgniteLogger log;

    /** */
    private final SocketChannel channel;

    /** */
    private final ObjectInput dis;

    /** */
    private final ObjectOutput dos;

    /** */
    private final IgniteInternalFuture<?> fut;

    /** If the channel is not been used anymore. */
    private final AtomicBoolean stopped = new AtomicBoolean();

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     * @throws IgniteCheckedException If fails.
     */
    public FileIoChannel(
        GridKernalContext ktx,
        IgniteSocketChannel channel,
        IgniteInternalFuture<?> fut
    ) throws IgniteCheckedException {
        assert channel.config().blocking();

        try {
            this.channel = channel.channel();
            this.dis = new ObjectInputStream(this.channel.socket().getInputStream());
            this.dos = new ObjectOutputStream(this.channel.socket().getOutputStream());
            this.log = ktx.log(getClass());
            this.fut = fut;
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Error with IO channel initialization", e);
        }
    }

    /**
     * @param meta The file meta to write from.
     * @throws IOException If fails.
     */
    private void doWriteMeta(IoMeta meta) throws IOException {
        writeNext((byte)1); // Says the remote to expect the next single file.

        meta.writeExternal(dos);
        dos.flush();
    }

    /**
     * @param file The region file object to write to.
     * @throws IgniteCheckedException If fails.
     */
    public void doWrite(IoMeta meta, FileSegment file) throws IgniteCheckedException {
        if (file.count() == 0)
            return; // Nothing to send.

        try {
            doWriteMeta(meta);

            if (log.isDebugEnabled())
                log.debug("The file meta info have been written [file=" + file + ']');

            // Send the whole file to channel.
            // Todo limit thransfer speed online
            long sent;

            file.open();

            while (file.transferred() < file.count() && !fut.isDone()) {
                sent = file.transferTo(channel);

                if (sent == -1)
                    checkFileEOF(file);
            }

            //Waiting for the writing response.
//            readAck();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("The file write error: " + file, e);
        }
        finally {
            U.closeQuiet(file);
        }
    }

    /**
     * @param meta The meta to read to.
     * @return {@code true} if the data read successfully.
     * @throws IgniteCheckedException If fails.
     */
    protected boolean doReadMeta(IoMeta meta) throws IgniteCheckedException {
        try {
            if (!hasNext()) {
                closeQuiet0();

                return false;
            }

            meta.readExternal(dis);

            if (log.isDebugEnabled())
                log.debug("The file meta info have been received [meta=" + meta + ']');

            return true;
        }
        catch (EOFException e) {
            throw new IgniteCheckedException("Input connection closed unexpectedly", e);
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteCheckedException("Read file meta error", e);
        }
    }

    /**
     * @param file The region file object to read to.
     * @param offset The position to start current iteration at.
     * @param count The number of bytes to read.
     * @throws IgniteCheckedException If fails.
     */
    public void doRead(FileSegment file, long offset, long count) throws IgniteCheckedException {
        if (file.count() <= 0)
            return; // Nothing to read.

        assert offset == file.position() + file.transferred();
        assert count == file.count() - offset;

        try {
            file.open();

            long readed;

            while (file.transferred() < file.count() && !fut.isDone()) {
                readed = file.transferFrom(channel);

                if (readed == -1)
                    checkFileEOF(file);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("The file read error: " + file, e);
        }
        finally {
            U.closeQuiet(file);
        }
    }

    /**
     * @param buff Buffer to read data into.
     * @return The number of bytes read, possibly zero, or <tt>-1</tt> if the channel has reached end-of-stream.
     * @throws IgniteCheckedException If fails.
     */
    public int doReadRaw(ByteBuffer buff) throws IgniteCheckedException {
        try {
            if (fut.isDone())
                return -1;

            return channel.read(buff);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @return {@code true} if all data have been transferred.
     * @throws IOException If fails.
     */
    private boolean hasNext() throws IOException {
        byte tomb = dis.readByte();

        return tomb != TOMB_STONE;
    }

    /**
     * @param stone The mile stone to write.
     * @throws IOException If fails.
     */
    private void writeNext(byte stone) throws IOException {
        dos.writeByte(stone);
        dos.flush();
    }

    /**
     * @param file The file object to check.
     * @throws EOFException If the check fails.
     */
    private static void checkFileEOF(FileSegment file) throws EOFException {
        if (file.transferred() < file.count()) {
            throw new EOFException("The file expected to be fully transferred [count=" + file.count() +
                ", transferred=" + file.transferred() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (stopped.compareAndSet(false, true)) {
            try {
                writeNext(TOMB_STONE);
            }
            catch (IOException e) {
                // Ignore.

                if (log.isDebugEnabled())
                    log.debug("Error writing tombstore to remote: " + e.getMessage());
            }
            finally {
                closeQuiet0();
            }
        }
    }

    /**
     * Perform close channel operation.
     */
    private void closeQuiet0() {
        U.closeQuiet(dos);
        U.closeQuiet(dis);
        U.closeQuiet(channel);
    }
}
