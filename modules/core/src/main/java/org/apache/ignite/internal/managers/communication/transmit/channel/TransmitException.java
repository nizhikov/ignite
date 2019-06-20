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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

/**
 * The class represents an exception on the transport level transmission (not the handler one). For instance,
 * case when the remote peer has closed the connection correctly, so read or write operation over given
 * socket channel will throw an {@link IOException} and it will be converted to this one exception.
 *
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
public class TransmitException extends IOException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs an {@code TransmitException} with the specified detail message.
     */
    public TransmitException(String msg) {
        super(msg);
    }
}
