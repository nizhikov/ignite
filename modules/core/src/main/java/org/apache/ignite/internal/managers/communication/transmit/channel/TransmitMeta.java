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

package org.apache.ignite.internal.managers.communication.transmit.channel;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Class represents a file meta information to send to remote node. Used to initiate a new file transfer
 * process or to continue the previous unfinished from the last transmission point.
 */
public class TransmitMeta implements Externalizable {
    /** Default transmit meta. */
    public static final TransmitMeta DFLT_TRANSMIT_META = new TransmitMeta();

    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private static final String UNNAMED_META = "unnamed";

    /**
     * The name to associate particular meta with.
     * Can be the particular file name, or an a transfer session identifier.
     */
    private String name;

    /** */
    private long offset;

    /** */
    private long cnt;

    /** The initial meta info for the file transferred the first time. */
    private boolean initial;

    /** */
    private HashMap<String, Serializable> map = new HashMap<>();

    /**
     *
     */
    public TransmitMeta() {
        this(UNNAMED_META);
    }

    /**
     * @param name The name to associate particular meta with.
     */
    public TransmitMeta(String name) {
        this(name, -1, -1, true, null);
    }

    /**
     * @param name The string name representation to assoticate particular meta with.
     * @param offset The start position of file.
     * @param cnt The amount of bytes to receive.
     * @param initial {@code true} if
     * @param params The additional transfer meta params.
     */
    public TransmitMeta(
        String name,
        long offset,
        long cnt,
        boolean initial,
        Map<String, Serializable> params
    ) {
        this.name = Objects.requireNonNull(name);
        this.offset = offset;
        this.cnt = cnt;
        this.initial = initial;

        if (params != null) {
            for (Map.Entry<String, Serializable> key : params.entrySet())
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
        return cnt;
    }

    /**
     * @return {@code true} if the file is transferred the first time.
     */
    public boolean initial() {
        return initial;
    }

    /**
     * @return The map of additional keys.
     */
    public Map<String, Serializable> params() {
        return Collections.unmodifiableMap(map);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name());
        out.writeLong(offset);
        out.writeLong(cnt);
        out.writeBoolean(initial);
        out.writeObject(map);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = Objects.requireNonNull(in.readUTF());
        offset = in.readLong();
        cnt = in.readLong();
        initial = in.readBoolean();
        map = (HashMap)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TransmitMeta meta = (TransmitMeta)o;

        return offset == meta.offset &&
            cnt == meta.cnt &&
            initial == meta.initial &&
            name.equals(meta.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, offset, cnt, initial);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransmitMeta.class, this);
    }
}
