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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;

/**
 *
 */
public interface ChunkHandler {
    /**
     * @param name The file name on remote.
     * @param position The start position pointer of downloading file in original source.
     * @param count Total count of bytes to read from the original source.
     * @param params The additional transfer file description params.
     * @return The size of of {@link ByteBuffer} to read the input channel into.
     */
    public int begin(String name, long position, long count, Map<String, Serializable> params);

    /**
     * @param buff The data filled buffer.
     * @return {@code true} if the chunk of data have been successfully accepted.
     * @throws IgniteCheckedException If fails.
     */
    public boolean chunk(ByteBuffer buff) throws IgniteCheckedException;

    /**
     * @param params The additional handling channel description params.
     */
    public void end(Map<String, Serializable> params);
}
