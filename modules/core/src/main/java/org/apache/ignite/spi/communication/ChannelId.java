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

package org.apache.ignite.spi.communication;

import java.util.Objects;
import java.util.UUID;

/**
 * The unique channel identifier.
 */
public class ChannelId {
    /** */
    private final UUID remoteId;

    /** */
    private final int idx;

    /**
     * @param remoteId The remote node id.
     * @param idx The unique conneciton id to remote node.
     */
    public ChannelId(UUID remoteId, int idx) {
        this.remoteId = remoteId;
        this.idx = idx;
    }

    /**
     * @return The remote node id.
     */
    public UUID remoteId() {
        return remoteId;
    }

    /**
     * @return The unique conneciton id to remote node.
     */
    public int idx() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ChannelId id1 = (ChannelId)o;

        return idx == id1.idx &&
            remoteId.equals(id1.remoteId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(remoteId, idx);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ChannelId{" +
            "remoteId=" + remoteId +
            ", idx=" + idx +
            '}';
    }
}
