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

package org.apache.ignite.internal.processors.transfer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;

/**
 *
 */
public interface FileIoReadHandler {
    /**
     * @param nodeId The remote node id connected from.
     * @param sessionId The unique session id.
     * @param stop The flag to stop the stream processing.
     */
    public void created(UUID nodeId, String sessionId, AtomicBoolean stop);

    /**
     * @param name The file name transfer from.
     * @param keys The additional transfer file description keys.
     * @return The destination object to transfer data to. Can be the {@link File} or {@link ByteBuffer}.
     * @throws IgniteCheckedException If fails.
     */
    public Object acceptFileMeta(String name, Map<String, String> keys) throws IgniteCheckedException;

    /**
     * @param file The file to read channel into.
     */
    public void accept(File file);

    /**
     * @param buff The buffer to read channel into.
     */
    public void accept(ByteBuffer buff);

    /**
     * @param cause The case of fail handling process.
     */
    public void exceptionCaught(Throwable cause);
}
