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
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitMeta;

/**
 * Class represents a stream of chunks, can be closed when the stream ends.
 */
public interface ChunkedStream extends Closeable {
    /**
     * @return The string of chunked IO stream.
     */
    public String name();

    /**
     * @return The bytes which has been transfered.
     */
    public long transferred();

    /**
     * @return The number of bytes to transfer (read from or write to).
     */
    public long count();

    /**
     * @return The size of chunk in bytes.
     */
    public int chunkSize();

    /**
     * @return Additional stream params
     */
    public Map<String, Serializable> params();

    /**
     * @return Stream meta information.
     */
    public TransmitMeta meta();

    /**
     * @throws IOException If the check fails.
     */
    public void checkStreamEOF() throws IOException;

    /**
     * @return {@code true} if and only if the chunked stream received all the data it expected.
     */
    public boolean endStream();
}
