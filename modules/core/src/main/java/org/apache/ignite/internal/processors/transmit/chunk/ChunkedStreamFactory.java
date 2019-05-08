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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.transmit.ReadPolicy;
import org.apache.ignite.internal.processors.transmit.TransmitSession;

/**
 *
 */
public class ChunkedStreamFactory {
    /**
     * @param policy The policy of how to read stream.
     * @param session The current session instance produces handlers.
     * @param chunkSize The size of chunk to read.
     * @return The chunked stream instance.
     * @throws IgniteCheckedException If fails.
     */
    public ChunkedInputStream createInputStream(
        ReadPolicy policy,
        TransmitSession session,
        int chunkSize
    ) throws IgniteCheckedException {
        ChunkedInputStream stream;

        switch (policy) {
            case FILE:
                stream = new ChunkedFileStream(session.fileHandler(), chunkSize);

                break;

            case BUFF:
                stream = new ChunkedBufferStream(session.chunkHandler(), chunkSize);

                break;

            default:
                throw new IgniteCheckedException("The type of read policy is unknown. The impelentation " +
                    "required: " + policy);
        }

        return stream;
    }
}
