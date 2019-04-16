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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class DefaultIoMeta implements IoMeta {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private String name;

    /** */
    private Long offset;

    /** */
    private Long count;

    /** */
    public DefaultIoMeta() {
        this(null, null, null);
    }

    /**
     * @param name The file name string representation.
     * @param offset The start position of file.
     * @param count The amount of bytes to receive.
     */
    public DefaultIoMeta(String name, Long offset, Long count) {
        this.name = name;
        this.offset = offset;
        this.count = count;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return Objects.requireNonNull(name);
    }

    /** {@inheritDoc} */
    @Override public long offset() {
        return Objects.requireNonNull(offset);
    }

    /** {@inheritDoc} */
    @Override public long count() {
        return Objects.requireNonNull(count);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        if (name == null || offset == null || count == null)
            throw new IOException("File meta information incorrect: " + this);

        out.writeUTF(name);
        out.writeLong(offset);
        out.writeLong(count);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        offset = in.readLong();
        count = in.readLong();

        if (name == null || offset == null || count == null)
            throw new IOException("File meta information incorrect: " + this);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DefaultIoMeta.class, this);
    }
}
