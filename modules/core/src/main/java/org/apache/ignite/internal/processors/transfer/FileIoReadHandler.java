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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
public interface FileIoReadHandler {
    /**
     * @param nodeId The remote node id connected from.
     * @param channel The channel connection to handle.
     */
    public void created(UUID nodeId, IgniteSocketChannel channel);

    /**
     * @param meta The meta file info to handle to.
     * @return The destination object to transfer data to.
     * @throws IgniteCheckedException If fails.
     */
    public Object accept(IoMeta meta) throws IgniteCheckedException;

    /**
     * @param file The file to read channel into.
     * @throws IgniteCheckedException If fails.
     */
    public void accept(File file) throws IgniteCheckedException;

    /**
     * @param buff The buffer to read channel into.
     * @throws IgniteCheckedException If fails.
     */
    public void accept(ByteBuffer buff) throws IgniteCheckedException;
}
