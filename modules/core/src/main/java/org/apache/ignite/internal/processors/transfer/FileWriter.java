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
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;

/**
 *
 */
public interface FileWriter extends AutoCloseable {
    /**
     * @param file The source file to send at.
     * @param offset The position to start at.
     * @param count The number of bytes to transfer.
     * @param params The additional transfer file description keys.
     * @throws IgniteCheckedException If fails.
     */
    public void write(File file, long offset, long count, Map<String, String> params) throws IgniteCheckedException;
}
