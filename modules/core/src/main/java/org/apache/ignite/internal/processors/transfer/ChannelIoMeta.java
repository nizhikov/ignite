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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class ChannelIoMeta implements Externalizable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** The tombstone key */
    @GridToStringExclude
    private static final ChannelIoMeta TOMBSTONE = new ChannelIoMeta("d2738352-8813-477a-a165-b73249798134", -1, -1, true, null);

    /**
     * The name to associate particular meta with.
     * Can be the particular file name, or an a transfer session identifier.
     */
    private String name;

    /** The initial meta info for the file transferred the first time. */
    private boolean initial;

    /** */
    private long offset;

    /** */
    private long count;

    /** */
    private HashMap<String, String> map = new HashMap<>();

    /** */
    public ChannelIoMeta() {
        this(null);
    }

    /**
     * @param name The name to associate particular meta with.
     */
    public ChannelIoMeta(String name) {
        this(name, -1, -1, false, null);
    }

    /**
     * @param name The string name representation to assoticate particular meta with.
     * @param offset The start position of file.
     * @param count The amount of bytes to receive.
     * @param initial {@code true} if
     * @param params The additional transfer meta params.
     */
    public ChannelIoMeta(String name, long offset, long count, boolean initial, Map<String, String> params) {
        this.name = name;
        this.initial = initial;
        this.offset = offset;
        this.count = count;

        if (params != null) {
            for (Map.Entry<String, String> key : params.entrySet())
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
     * @return {@code true} if the file is transferred the first time.
     */
    public boolean initial() {
        return initial;
    }

    /**
     * @return The position to start channel transfer at.
     */
    public long offset() {
        assert offset >=0;

        return offset;
    }

    /**
     * @return The number of bytes expect to transfer.
     */
    public long count() {
        assert count >= 0;

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
    public static ChannelIoMeta tombstone() {
        return TOMBSTONE;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name());
        out.writeBoolean(initial);
        out.writeLong(offset);
        out.writeLong(count);
        out.writeObject(map);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = Objects.requireNonNull(in.readUTF());
        initial = in.readBoolean();
        offset = in.readLong();
        count = in.readLong();
        map = (HashMap) in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChannelIoMeta.class, this);
    }
}
