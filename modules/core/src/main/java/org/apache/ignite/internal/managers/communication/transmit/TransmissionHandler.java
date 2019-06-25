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

package org.apache.ignite.internal.managers.communication.transmit;

import java.nio.channels.FileChannel;
import java.util.UUID;

/**
 * The {@code TransmissionHandler} represents a single session of handling input file transmission requests.
 */
public interface TransmissionHandler {
    /**
     * @param nodeId The remote node id receive request for transmission from.
     */
    public void onBegin(UUID nodeId);

    /**
     * @param err The err of fail handling process.
     */
    public void onException(Throwable err);

    /**
     * The end of session transmission process.
     */
    public void onEnd(UUID nodeId);

    /**
     * @return The instance of read handler to process incoming data by chunks.
     */
    public ChunkHandler chunkHandler(UUID nodeId);

    /**
     * @return The intance of read handler to process incoming data like the {@link FileChannel} manner.
     */
    public FileHandler fileHandler(UUID nodeId);
}
