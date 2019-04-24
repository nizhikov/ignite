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

package org.apache.ignite.internal.processors.transmit;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;

/**
 * The statefull read file handler from the remote channel.
 */
public interface FileReadHandler {
    /**
     * @param nodeId The remote node id connected from.
     * @param sessionId The unique session id.
     */
    public void init(UUID nodeId, String sessionId);

    /**
     * @param name The file name transfer from.
     * @param plc The type of policy to handle source data.
     * @param keys The additional transfer file description keys.
     * @return The absolute pathname string denoting the file or {@code null} if there is no sense.
     * @throws IgniteCheckedException If fails.
     */
    public String begin(String name, ReadPolicy plc, Map<String, Serializable> keys) throws IgniteCheckedException;

    /**
     * @param file The destination file to read data into.
     * @param chunkPos The start position of particular chunk in the original source.
     * @param chunkSize The size of particular chunk in the original source.
     * @return {@code true} if the chunk of data have been successfully accepted.
     */
    public boolean acceptChunk(File file, long chunkPos, long chunkSize);

    /**
     * @param buff The destination buffer to read data into.
     * @param chunkPos The start position of particular chunk in the original source.
     * @param chunkSize The size of particular chunk in the original source.
     * @return {@code true} if the chunk of data have been successfully accepted.
     */
    public boolean acceptChunk(ByteBuffer buff, long chunkPos, long chunkSize);

    /**
     * @param position The start position pointer of download file in original source.
     * @param count Total count of bytes readed from the original source.
     */
    public void end(long position, long count);

    /**
     * @param cause The case of fail handling process.
     */
    public void onException(Throwable cause);
}
