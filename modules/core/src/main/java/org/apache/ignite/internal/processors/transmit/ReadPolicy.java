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
 *
 */
public enum ReadPolicy {
    /** Read the source direcly info the FileChannel. */
    FILE(0, File.class),

    /** Read the source into the appropriate ByteBuffer. */
    BUFF(1, ByteBuffer.class);

    /** The type of handler to read source. */
    private int type;

    /** */
    private Class<?> clazz;

    /**
     * @param type The type of read handler.
     */
    ReadPolicy(int type, Class<?> clazz) {
        this.type = type;
        this.clazz = clazz;
    }

    /**
     * @return The type of read handler.
     */
    public int type() {
        return type;
    }

    /**
     * @return The type of particualr handler.
     */
    public Class<?> clazz() {
        return clazz;
    }
}
