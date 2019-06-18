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

package org.apache.ignite.internal.managers.communication.transmit.chunk;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.transmit.channel.InputTransmitChannel;

/**
 * Class represents an object which can be read from a channel by chunks of
 * predefined size. Closes when a transmission of represented object ends.
 */
public interface ReadableChunkedObject extends Closeable {
    /**
     * @return Name of chunked object.
     */
    public String name();

    /**
     * @return Number of bytes which has been transfered.
     */
    public long transferred();

    /**
     * @return Start chunked object position (same as a file offset) .
     */
    public long startPosition();

    /**
     * @return Number of bytes to transfer (read from or write to channel).
     */
    public long count();

    /**
     * @return Size of each chunk in bytes.
     */
    public int chunkSize();

    /**
     * @return Additional chunekd object params.
     */
    public Map<String, Serializable> params();

    /**
     * @return {@code true} if and only if a chunked object has received all the data it expects.
     */
    public boolean transmitEnd();

    /**
     * @param in Channel to read data from.
     * @throws IOException If read meta input failed.
     * @throws IgniteCheckedException If validation failed.
     */
    public void setup(InputTransmitChannel in) throws IOException, IgniteCheckedException;

    /**
     * @param in Channel to read data from.
     * @throws IOException If fails.
     */
    public void readChunk(InputTransmitChannel in) throws IOException;
}
