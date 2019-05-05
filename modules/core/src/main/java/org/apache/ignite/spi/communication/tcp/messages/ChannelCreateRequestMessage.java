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

package org.apache.ignite.spi.communication.tcp.messages;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 * Message requesting to creation of {@link IgniteSocketChannel}.
 */
public class ChannelCreateRequestMessage implements Message {
    /** Request message type */
    public static final int CHANNEL_REQUEST_MSG_TYPE = -29;

    /** */
    private static final long serialVersionUID = 0L;

    /** The map of channel attributes. */
    @GridDirectTransient
    private Map<String, Serializable> attrs;

    /** Message. */
    @GridToStringExclude
    private byte[] attrsBytes;

    /**
     * @return The map of channel attributes.
     */
    public Map<String, Serializable> getAttrs() {
        return attrs;
    }

    /**
     * @param attrs The map of channel attributes.
     */
    public void setAttrs(Map<String, Serializable> attrs) {
        this.attrs = new HashMap<>(attrs);
    }

    /**
     * @return The serialized channel attributes byte array.
     */
    public byte[] getAttrsBytes() {
        return attrsBytes;
    }

    /**
     * @param attrsBytes The serialized channel attributes byte array.
     */
    public void setAttrsBytes(byte[] attrsBytes) {
        this.attrsBytes = attrsBytes;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        if (writer.state() == 0) {
            if (!writer.writeByteArray("attrsBytes", attrsBytes))
                return false;

            writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (reader.state() == 0) {
            attrsBytes = reader.readByteArray("attrsBytes");

            if (!reader.isLastRead())
                return false;

            reader.incrementState();
        }

        return reader.afterMessageRead(ChannelCreateRequestMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return CHANNEL_REQUEST_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChannelCreateRequestMessage.class, this);
    }

}
