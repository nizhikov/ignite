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

package org.apache.ignite.internal.processors.transmit.chunk;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.transmit.channel.RemoteTransmitException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
abstract class AbstractChunkedStream implements ChunkedStream {
    /** The unique input name to identify particular transfer part.*/
    private final String name;

    /** The position offest to start at. */
    private final long startPos;

    /** The total number of bytes to send. */
    private final long count;

    /** The size of segment for the read. */
    private final int chunkSize;

    /** Additional stream params. */
    @GridToStringInclude
    private final Map<String, Serializable> params;

    /** The number of bytes successfully transferred druring iteration. */
    @GridToStringInclude
    protected final AtomicLong transferred = new AtomicLong();

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     * @param chunkSize The size of chunk to read.
     * @param params Additional stream params.
     */
    protected AbstractChunkedStream(
        String name,
        long startPos,
        long count,
        int chunkSize,
        Map<String, Serializable> params
    ) {
        assert startPos >= 0 : "The file position must be non-negative: " + startPos;
        assert count >= 0 : "The number of bytes to sent must be positive: " + count;
        assert params != null;

        this.name = Objects.requireNonNull(name);
        this.startPos = startPos;
        this.count = count;
        this.chunkSize = chunkSize;
        this.params = Collections.unmodifiableMap(new HashMap<>(params));
    }

    /** {@inheritDoc} */
    @Override public long startPosition() {
        return startPos;
    }

    /** {@inheritDoc} */
    @Override public long transferred() {
        return transferred.get();
    }

    /** {@inheritDoc} */
    @Override public void transferred(long cnt) {
        transferred.set(cnt);
    }

    /** {@inheritDoc} */
    @Override public long count() {
        return count;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int chunkSize() {
        return chunkSize;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Serializable> params() {
        return params;
    }

    /**
     * @param io The file object to check.
     * @throws IOException If the check fails.
     */
    public void checkChunkedIoEOF(ChunkedStream io) throws IOException {
        if (io.transferred() < io.count()) {
            throw new RemoteTransmitException("Socket channel EOF received, but the file is not fully transferred " +
                "[count=" + io.count() + ", transferred=" + io.transferred() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public boolean endOfStream() {
        return transferred.get() == count;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractChunkedStream.class, this);
    }
}
