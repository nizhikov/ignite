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

package org.apache.ignite.spi.communication.tcp.internal.channel;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * A channel configuration for the {@link Channel}.
 */
public final class ChannelConfig {
    /** */
    private final SocketChannel channel;

    /** */
    private final Socket socket;

    /**
     * @param channel The socket channel to create configuration from.
     */
    public ChannelConfig(SocketChannel channel) {
        this.channel = channel;
        this.socket = channel.socket();
    }

    /**
     * @return
     */
    public boolean blocking() {
        return channel.isBlocking();
    }

    /**
     * @param blocking
     * @return
     */
    public ChannelConfig blocking(boolean blocking) {
        try {
            channel.configureBlocking(blocking);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /**
     * @return
     */
    public int timeout() {
        try {
            return socket.getSoTimeout();
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param millis
     * @return
     */
    public ChannelConfig timeout(int millis) {
        try {
            socket.setSoTimeout(millis);
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChannelConfig.class, this);
    }
}
