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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * The {@code FileHandler} represents by itself the way of input data stream processing. All the data will
 * be processed under the hood using zero-copy transferring algorithm and only start file processing and
 * the end of processing will be provided.
 */
public interface FileHandler {
    /**
     * @param name File name transferred from remote.
     * @param params The additional transfer file description params.
     * @return The absolute pathname string denoting the file or {@code null} if it is no sense.
     * @throws IOException If fails.
     */
    public String path(String name, Map<String, Serializable> params) throws IOException;

    /**
     * @param file File with fully downloaded data into.
     * @param offset Offset pointer of downloaded file in original source.
     * @param cnt Number of bytes transferred from source started from given offset.
     * @param params Additional transfer file description params.
     */
    public void accept(File file, long offset, long cnt, Map<String, Serializable> params);
}
