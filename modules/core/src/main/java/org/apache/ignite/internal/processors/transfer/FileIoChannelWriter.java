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
import org.apache.ignite.IgniteCheckedException;

/**
 *
 */
public interface FileIoChannelWriter extends AutoCloseable {
    /**
     * @param file The source file to send at.
     * @param meta The additional transfer file meta data.
     * @param offset The position to start at.
     * @param count The number of bytes to transfer.
     * @throws IgniteCheckedException If fails.
     */
    public void write(File file, IoMeta meta, long offset, long count) throws IgniteCheckedException;
}
