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

package org.apache.ignite.internal.processors.transmit.chunk;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import org.apache.ignite.internal.processors.transmit.stream.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.stream.TransmitOutputChannel;

/**
 *
 */
public interface ChunkedStream extends Closeable {
    /** The default chunk size to transfer data. */
    public static final int DFLT_SEGMENT_SIZE = 1024 * 1024;

    /**
     * @return The string of chunked IO stream.
     */
    public String name();

    /**
     * @return The position from which the transfer will start. For the {@link File} it will be offset
     * in the file where the transfer begin data transfer.
     */
    public long startPosition();

    /**
     * @return The bytes which has been transfered.
     */
    public long transferred();

    /**
     * @return The number of bytes to transfer (read from or write to).
     */
    public long count();

    /**
     * @throws IOException If initialization failed.
     */
    public void init() throws IOException;

    /**
     * @param ch The channel to read data from.
     * @throws IOException If fails.
     */
    public void readChunk(TransmitInputChannel ch) throws IOException;

    /**
     * @param ch The channel to write data into.
     * @throws IOException If fails.
     */
    public void writeChunk(TransmitOutputChannel ch) throws IOException;

    /**
     * @return {@code true} if and only if the chunked stream received all the data it expected.
     */
    public boolean endOfStream();
}
