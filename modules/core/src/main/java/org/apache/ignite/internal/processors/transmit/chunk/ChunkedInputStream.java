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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.transmit.channel.TransmitInputChannel;

/**
 *
 */
public interface ChunkedInputStream extends ChunkedStream {
    /**
     * @param in The channel to read data from.
     * @throws IOException If failed.
     * @throws IgniteCheckedException If failed.
     */
    public void setup(TransmitInputChannel in) throws IOException, IgniteCheckedException;

    /**
     * @param in The channel to read data from.
     * @throws IOException If fails.
     */
    public void readChunk(TransmitInputChannel in) throws IOException;
}
