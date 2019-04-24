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
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 * If the peer has closed the connection in an orderly way
 *
 * read() returns -1
 * readLine() returns null
 * readXXX() throws EOFException for any other XXX.
 *
 * A write will throw an IOException: 'connection reset by peer', eventually, subject to buffering delays.
 */
public abstract class TransmitAbstractChannel implements AutoCloseable {
    /** */
    private static final String RESET_BY_PEER_MSG = "Connection reset by peer";

    /** */
    private final IgniteSocketChannel igniteChannel;

    /** */
    protected final IgniteLogger log;

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     */
    protected TransmitAbstractChannel(
        GridKernalContext ktx,
        IgniteSocketChannel channel
    ) {
        assert channel.config().blocking();

        this.igniteChannel = channel;
        this.log = ktx.log(getClass());
    }

    /**
     * @param cause The original cause to throw.
     * @return The new cause or the old one.
     */
    public IOException transformExceptionIfNeed(IOException cause) {
        // Transform the connection reset by peer error message.
        if ((cause instanceof IOException && RESET_BY_PEER_MSG.equals(cause.getMessage())) ||
            cause instanceof EOFException ||
            cause instanceof ClosedChannelException ||
            cause instanceof AsynchronousCloseException ||
            cause instanceof ClosedByInterruptException) {
            // Return the new one with detailed message.
            return new RemoteTransmitException(
                "Lost connection to the remote node. The connection will be re-established according " +
                    "to the manager's transmission configuration [remoteId=" + igniteChannel.id().remoteId() +
                    ", index=" + igniteChannel.id().idx() + ']', cause);
        }

        return cause;
    }

    /**
     * @return The corresponding ignite channel.
     */
    public IgniteSocketChannel igniteSocket() {
        return igniteChannel;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(igniteChannel);
    }
}
