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

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @param <T> The type of destination object.
 */
public class FileTarget<T> {
    /** */
    private final T target;

    /**
     * @param target The target object to read source into.
     */
    private FileTarget(T target) {
        this.target = target;
    }

    /**
     * @param obj The buffer object to read source into.
     * @return The target wrapper instance over the buffer object.
     */
    public static FileTarget<ByteBuffer> buffTarget(ByteBuffer obj) {
        return new FileTarget<>(obj);
    }

    /**
     * @param obj The file to read source into.
     * @return The target wrapper instance over the file.
     */
    public static FileTarget<File> fileTarget(File obj) {
        return new FileTarget<>(obj);
    }

    /**
     * @return The destination object to read source into.
     */
    public T target() {
        return target;
    }
}
