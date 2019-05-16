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

package org.apache.ignite.internal.processors.transmit.channel;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.transmit.ReadPolicy;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 * Class represents an input transmission connection channel.
 * <p>
 * Please, see {@link TransmitAbstractChannel} fot details.
 */
public class TransmitInputChannel extends TransmitAbstractChannel {
    /** */
    @GridToStringExclude
    private final ObjectInput dis;

    /**
     * @param ktx Kernal context.
     * @param igniteChannel Ignite channel to upload files to.
     * @throws IOException If fails.
     */
    public TransmitInputChannel(
        GridKernalContext ktx,
        IgniteSocketChannel igniteChannel
    ) throws IOException {
        super(ktx, igniteChannel);

        dis = new ObjectInputStream(igniteChannel.channel().socket().getInputStream());
    }

    /**
     * @return Readed {@link ReadPolicy} from channel.
     * @throws IOException If fails.
     */
    public ReadPolicy readPolicy() throws IOException {
        try {
            int plc = dis.readInt();

            if (plc > ReadPolicy.values().length)
                throw new IOException("The policy received from channel is unknown [order=" + plc + ']');

            return ReadPolicy.values()[plc];
        }
        catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @return hash The hash of transmitted data.
     * @throws IOException If fails.
     */
    public long acknowledge() throws IOException {
        try {
            return dis.readLong();
        }
        catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @param meta The meta to read to.
     * @throws IOException If fails.
     */
    public void readMeta(TransmitMeta meta) throws IOException {
        try {
            meta.readExternal(dis);

            if (log.isDebugEnabled())
                log.debug("The file meta info have been received [meta=" + meta + ']');
        }
        catch (EOFException e) {
            throw transformExceptionIfNeed(e);
        }
        catch (ClassNotFoundException e) {
            throw new IOException("The required transmit meta class information not found", e);
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
        try {
            return fileIO.transferFrom((ReadableByteChannel)igniteChannel().channel(), position, count);
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
    public long readInto(ByteBuffer buff) throws IOException {
        return igniteChannel().channel().read(buff);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        super.close();

        U.closeQuiet(dis);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransmitInputChannel.class, this, "super", super.toString());
    }
}
