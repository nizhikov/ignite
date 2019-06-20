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

package org.apache.ignite.internal.managers.communication.transmit.channel;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class represents an input transmission connection channel.
 * <p>
 * Please, see {@link AbstractTransmitChannel} fot details.
 */
public class InputTransmitChannel extends AbstractTransmitChannel {
    /** Decoreated with data operations socket of input channel. */
    @GridToStringExclude
    private final ObjectInput ois;

    /**
     * @param log Ignite logger.
     * @param channel Socket channel to read data from.
     * @throws IOException If channel configuration fails.
     */
    public InputTransmitChannel(
        IgniteLogger log,
        SocketChannel channel
    ) throws IOException {
        super(log, channel);

        ois = new ObjectInputStream(channel.socket().getInputStream());
    }

    /**
     * @return Readed int value from channel.
     * @throws IOException If fails.
     */
    public int readInt() throws IOException {
        try {
            return ois.readInt();
        }
        catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @return The hash of transmitted data.
     * @throws IOException If fails.
     */
    public long acknowledge() throws IOException {
        try {
            return ois.readLong();
        }
        catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @param meta Meta instance to read channel data to.
     * @throws IOException If fails.
     */
    public void readMeta(TransmitMeta meta) throws IOException {
        try {
            meta.readExternal(ois);

            if (log.isDebugEnabled())
                log.debug("The file meta info have been received [meta=" + meta + ']');
        }
        catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @param fileIO The I\O file.
     * @param pos The pos to start from.
     * @param cnt The number of bytes to read.
     * @return The number of actually readed bytes.
     * @throws IOException If fails.
     */
    public long read(FileIO fileIO, long pos, long cnt) throws IOException {
        try {
            return fileIO.transferFrom((ReadableByteChannel)channel(), pos, cnt);
        }
        catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @param buff Buffer to read data into.
     * @return The number of bytes read, possibly zero, or <tt>-1</tt> if the channel has reached end-of-stream.
     * @throws IOException If fails.
     */
    public long read(ByteBuffer buff) throws IOException {
        try {
            return channel().read(buff);
        }
        catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        super.close();

        U.closeQuiet(ois);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InputTransmitChannel.class, this, "super", super.toString());
    }
}
