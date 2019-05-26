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

import java.io.Closeable;
import java.io.Serializable;
import java.nio.channels.SocketChannel;
import java.util.Map;

/**
 * A hub to a direct communication between ignite components which is capable of I/O operations such as read, write.
 */
public interface Channel extends Closeable {
    /**
     * @return The channel's configuration.
     */
    public ChannelConfig config();

    /**
     * @return A map of all channel attributes.
     */
    public Map<String, Serializable> attrs();

    /**
     * @param name The name to get attribute.
     * @param <T> The attribute type.
     * @return The corresponding channel attribute instance.
     */
    public <T extends Serializable> T attr(String name);

    /**
     * @param name The name to get attribute.
     * @param obj The chanel attribute instance.
     * @param <T> The attribute type.
     */
    public <T extends Serializable> void attr(String name, T obj);

    /**
     * @return The underlying java nio {@link SocketChannel} used by the current channel.
     */
    public SocketChannel socket();

    /**
     * @return <tt>true</tt> if the channel is configured and ready to use.
     */
    public boolean active();

    /**
     * Make the channel ready for use.
     */
    public void activate();
}
