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

import java.io.IOException;
import org.apache.ignite.internal.processors.transmit.stream.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.stream.TransmitOutputChannel;

/**
 * @param <T>
 */
public interface ChunkedIo<T> extends AutoCloseable {
    /**
     * @return The chunk of data to interact as io.
     */
    public T chunk();

    /**
     * @return The string representation file name.
     */
    public String name();

    /**
     * @return The offset in the file where the transfer began.
     */
    public long postition();

    /**
     * @return The bytes which was transfered already.
     */
    public long transferred();

    /**
     * @return The number of bytes to transfer.
     */
    public long count();

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
     * @return {@code true} if and only if there is no data left in the channel and it reached its end.
     */
    public boolean endOfTransmit();
}
