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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.transmit.ReadPolicy;
import org.apache.ignite.internal.managers.communication.transmit.FileTransmitHandler;

/**
 * Factory to create a new chunked object when a file read or write event requested.
 */
public class ChunkedObjectFactory {
    /**
     * @param nodeId Remote node id.
     * @param plc Policy of how to read input data stream.
     * @param ses The current handler instance which produces file handlers.
     * @return Chunked object instance.
     * @throws IgniteCheckedException If fails.
     */
    public InputChunkedObject createInputChunkedObject(
        UUID nodeId,
        ReadPolicy plc,
        FileTransmitHandler ses
    ) throws IgniteCheckedException {
        InputChunkedObject obj;

        switch (plc) {
            case FILE:
                obj = new InputChunkedFile(ses.fileHandler(nodeId));

                break;

            case BUFF:
                obj = new InputChunkedBuffer(ses.chunkHandler(nodeId));

                break;

            default:
                throw new IgniteCheckedException("The type of read plc is unknown. The impelentation " +
                    "required: " + plc);
        }

        return obj;
    }
}
