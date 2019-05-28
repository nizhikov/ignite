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

import java.nio.channels.FileChannel;
import java.util.UUID;

/**
 * The {@code TransmitSessionHandler} represents a single session of handling input file transmission requests.
 */
public interface TransmitSessionHandler {
    /**
     * @param nodeId The remote node id connected from.
     * @param sessionId The unique session id.
     */
    public void begin(UUID nodeId, String sessionId);

    /**
     * @param cause The cause of fail handling process.
     */
    public void onException(Throwable cause);

    /**
     * The end of session transmission process.
     */
    public void end();

    /**
     * @return The instance of read handler to process incoming data by chunks.
     */
    public ChunkHandler chunkHandler();

    /**
     * @return The intance of read handler to process incoming data like the {@link FileChannel} manner.
     */
    public FileHandler fileHandler();
}
