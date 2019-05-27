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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * <p>
 *     <h3>Channel exception handling</h3>
 *
 *     If the peer has closed the connection in an orderly way, the read operation:
 *     <ul>
 *         <li>read() returns -1</li>
 *         <li>readLine() returns null</li>
 *         <li>readXXX() throws EOFException for any other XXX</li>
 *     </ul>
 *     A write will throw an <tt>IOException</tt> 'Connection reset by peer', eventually, subject to buffering delays.
 * </p>
 * <p>
 *     <h3>Channel timeout handling</h3>
 *
 *     <ul>
 *         <li>For read operations over the {@link InputStream} or write operation through the {@link OutputStream}
 *         the {@link Socket#setSoTimeout(int)} will be used and an {@link SocketTimeoutException} will be
 *         thrown when the timeout occured.</li>
 *         <li>To achive the file zero-copy {@link FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)}
 *         the {@link SocketChannel} must be used directly in the blocking mode. For reading or writing over
 *         the SocketChannels, using the <tt>Socket#setSoTimeout(int)</tt> is not possible, because it isn't
 *         supported for sockets originating as channels. In this case, the decicated wather thread must be
 *         used which will close conneciton on timeout occured.</li>
 *     </ul>
 * </p>
 */
public abstract class TransmitAbstractChannel implements Closeable {
    /** */
    private static final int DFLT_IO_TIMEOUT_MILLIS = 5_000;

    /** */
    private static final String RESET_BY_PEER_MSG = "Connection reset by peer";

    /** */
    private static final String CLOSED_BY_REMOTE_MSG = "An existing connection was forcibly closed by the remote host";

    /** */
    private final SocketChannel channel;

    /** */
    protected final IgniteLogger log;

    /** */
    private final int timeoutMillis;

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     */
    protected TransmitAbstractChannel(
        GridKernalContext ktx,
        SocketChannel channel
    ) throws IOException {
        this(ktx, channel, DFLT_IO_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     * @param timeout Read\write timeout.
     * @param unit The {@link TimeUnit} of given <tt>timeout</tt>.
     * @throws IOException If channel configuration fails.
     */
    protected TransmitAbstractChannel(
        GridKernalContext ktx,
        SocketChannel channel,
        int timeout,
        TimeUnit unit
    ) throws IOException {
        assert ktx != null;
        assert channel != null;
        assert unit != null;

        this.channel = channel;
        log = ktx.log(getClass());
        timeoutMillis = timeout <= 0 ? 0 : Math.max((int)unit.toMillis(timeout), DFLT_IO_TIMEOUT_MILLIS);

        // Timeout must be enabled prior to entering the blocking mode to have effect.
        channel.socket().setSoTimeout(timeoutMillis);
        channel.configureBlocking(true);
    }

    /**
     * @return The timeout of read\write operations in milliseconds.
     */
    public int timeout() {
        return timeoutMillis;
    }

    /**
     * @param cause The original cause to throw.
     * @return The new cause or the old one.
     */
    public IOException transformExceptionIfNeed(IOException cause) {
        // The set of local issues with connection.
        if (cause instanceof EOFException ||
            cause instanceof ClosedChannelException ||
            cause instanceof ClosedByInterruptException) {
            // Return the new one with detailed message.
            return new RemoteTransmitException(
                "Lost connection to the remote node. The connection will be re-established according " +
                    "to the manager's transmission configuration [socket=" + socket() + ']', cause);
        }
        // Connection timeout issues.
        else if (cause instanceof SocketTimeoutException ||
            cause instanceof AsynchronousCloseException) {
            return new RemoteTransmitException(
                "The connection has been timeouted. The connection will be re-established according " +
                    "to the manager's transmission configuration [socket=" + socket() + ']', cause);
        }
        else if (cause instanceof IOException) {
            // Improve IOException connection error handling
            String causeMsg = cause.getMessage();

            if (RESET_BY_PEER_MSG.equals(causeMsg) ||
                CLOSED_BY_REMOTE_MSG.equals(causeMsg)) {
                return new RemoteTransmitException("Connection has been dropped by remote due to unhandled error. " +
                    "The connection will be re-established according to the manager's transmission configuration " +
                    "[socket=" + socket() + ']', cause);
            };
        }

        return cause;
    }

    /**
     * @return The corresponding ignite channel.
     */
    public SocketChannel socket() {
        return channel;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(channel);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransmitAbstractChannel.class, this);
    }
}
