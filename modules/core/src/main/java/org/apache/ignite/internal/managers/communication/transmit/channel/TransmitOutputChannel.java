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

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class represents an output transmission connection channel.
 * <p>
 * Please, see {@link TransmitAbstractChannel} fot details.
 */
public class TransmitOutputChannel extends TransmitAbstractChannel {
    /** Decorated with data operations socket of output channel. */
    private final ObjectOutput oos;

    /**
     * @param log Ignite logger.
     * @param channel Socket channel to expect data from.
     * @throws IOException If fails.
     */
    public TransmitOutputChannel(
        IgniteLogger log,
        SocketChannel channel
    ) throws IOException {
        super(log, channel);

        oos = new ObjectOutputStream(channel.socket().getOutputStream());
    }

    /**
     * @param intVal Value to write to channel.
     * @throws IOException If fails.
     */
    public void writeInt(int intVal) throws IOException {
        try {
            oos.writeInt(intVal);

            oos.flush();
        } catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @param hash The hash of transmitted data.
     * @throws IOException If fails.
     */
    public void acknowledge(long hash) throws IOException {
        try {
            oos.writeLong(hash);

            oos.flush();
        } catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @param meta The file meta to write from.
     * @throws IOException If fails.
     */
    public void writeMeta(TransmitMeta meta) throws IOException {
        try {
            meta.writeExternal(oos);

            oos.flush();

            if (log.isDebugEnabled())
                log.debug("The file meta info have been written:" + meta + ']');
        } catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /**
     * @param position The position to start from.
     * @param count The number of bytes to write.
     * @param fileIO The I\O file
     * @return The number of written bytes.
     * @throws IOException If fails.
     */
    public long writeFrom(long position, long count, FileIO fileIO) throws IOException {
        try {
            return fileIO.transferTo(position, count, (WritableByteChannel)channel());
        }
        catch (IOException e) {
            throw transformExceptionIfNeed(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        super.close();

        U.closeQuiet(oos);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransmitOutputChannel.class, this, "super", super.toString());
    }
}
