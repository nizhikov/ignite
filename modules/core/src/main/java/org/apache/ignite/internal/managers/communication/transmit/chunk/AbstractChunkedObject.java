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

package org.apache.ignite.internal.managers.communication.transmit.chunk;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.internal.managers.communication.transmit.util.TimedSemaphore;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Class represents base object which can we transferred (written or read) by chunks of
 * predefined size over a socket channel.
 */
abstract class AbstractChunkedObject implements Closeable {
    /** Additional stream params. */
    @GridToStringInclude
    protected final Map<String, Serializable> params = new HashMap<>();

    /**
     * Read opration speed limiter. Provide single permit per loaded byte
     * within <tt>1</tt> sec period of time.
     */
    protected TimedSemaphore limiter;

    /** Node stopping checker. */
    protected Supplier<Boolean> nodeStopped;

    /** The number of bytes successfully transferred druring iteration. */
    protected long transferred;

    /** The size of segment for the read. */
    private int chunkSize;

    /** The unique input name to identify particular transfer part. */
    protected String name;

    /**
     * The position from which the transfer will start. For the {@link File} it will be offset
     * where the transfer begin data transfer.
     */
    protected long startPos;

    /** The total number of bytes to send. */
    protected long cnt;

    /** Initialization completion flag. */
    protected boolean inited;

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     */
    protected AbstractChunkedObject(
        String name,
        long startPos,
        long cnt,
        Map<String, Serializable> params
    ) {
        this.name = name;
        this.startPos = startPos;
        this.cnt = cnt;

        if (params != null)
            this.params.putAll(params);
    }

    /**
     * @return Name of chunked object.
     */
    public String name() {
        return name;
    }

    /**
     * @return Start chunked object position (same as a file offset) .
     */
    public long startPosition() {
        return startPos;
    }

    /**
     * @return Number of bytes to transfer (read from or write to channel).
     */
    public long count() {
        return cnt;
    }

    /**
     * @return Size of each chunk in bytes.
     */
    public int chunkSize() {
        return chunkSize;
    }

    /**
     * @return Number of bytes which has been transfered.
     */
    public long transferred() {
        return transferred;
    }

    /**
     * @return Additional chunked object params.
     */
    public Map<String, Serializable> params() {
        return params;
    }

    /**
     * @param chunkSize The size of chunk in bytes.
     */
    protected void chunkSize(int chunkSize) {
        assert chunkSize > 0;

        this.chunkSize = chunkSize;
    }

    /**
     * @param cnt The number of bytes which has been already transferred.
     */
    protected void transferred(long cnt) {
        assert cnt >= 0;

        transferred = cnt;
    }

    /**
     * @return {@code true} if and only if a chunked object has received all the data it expects.
     */
    protected boolean hasNextChunk() {
        return transferred < cnt;
    }

    /**
     * @throws IOException If fails.
     */
    protected void checkTransferLimitCount() throws IOException {
        if (transferred > cnt) {
            throw new IOException("File has been transferred with incorrect size " +
                "[expect=" + cnt + ", actual=" + transferred + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractChunkedObject.class, this);
    }
}
