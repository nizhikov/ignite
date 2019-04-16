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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.print.DocFlavor;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class IoMeta implements Externalizable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** The tombstone key */
    private static final IoMeta TOMBSTONE = new IoMeta("f093hf90-234fh298-23ry9887dfs", -1, -1, null);

    /** */
    private String name;

    /** */
    private long offset;

    /** */
    private long count;

    /** */
    private HashMap<String, String> map = new HashMap<>();

    /** */
    public IoMeta() {
        this(null, -1, -1, null);
    }

    /**
     * @param name The file name string representation.
     * @param offset The start position of file.
     * @param count The amount of bytes to receive.
     */
    public IoMeta(String name, long offset, long count, Map<String, String> keys) {
        this.name = name;
        this.offset = offset;
        this.count = count;

        if (keys != null) {
            for (Map.Entry<String, String> key : keys.entrySet())
                map.put(key.getKey(), key.getValue());
        }
    }

    /**
     * The string representation name of particular file.
     */
    public String name() {
        return Objects.requireNonNull(name);
    }

    /**
     * @return The position to start channel transfer at.
     */
    public long offset() {
        return offset;
    }

    /**
     * @return The number of bytes expect to transfer.
     */
    public long count() {
        return count;
    }

    /**
     * @return The map of additional keys.
     */
    public Map<String, String> keys() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * @return The tombstone meta info.
     */
    public static IoMeta tombstone() {
        return TOMBSTONE;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        if (name == null || offset < 0 || count < 0)
            throw new IOException("File meta information incorrect: " + this);

        out.writeUTF(name);
        out.writeLong(offset);
        out.writeLong(count);
        out.writeObject(map);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        offset = in.readLong();
        count = in.readLong();
        map = (HashMap) in.readObject();

        if (name == null || offset < 0 || count < 0)
            throw new IOException("File meta information incorrect: " + this);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IoMeta.class, this);
    }
}
