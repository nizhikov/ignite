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

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 * Message response to creation of {@link Channel}.
 */
public class ChannelCreateResponse implements Message {
    /** Request message type (value is {@code 175}). */
    public static final short TYPE_CODE = 175;

    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (buf.remaining() < DIRECT_TYPE_SIZE)
            return false;

        TcpCommunicationSpi.writeMessageType(buf, directType());

        buf.put((byte)1);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChannelCreateResponse.class, this);
    }

}
