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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitInputChannel;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitMeta;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitOutputChannel;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * The chunked stream with the basic behaviour.
 */
abstract class AbstractChunkedStream implements ChunkedInputStream, ChunkedOutputStream {
    /** Additional stream params. */
    @GridToStringInclude
    private final Map<String, Serializable> params = new HashMap<>();

    /** The number of bytes successfully transferred druring iteration. */
    @GridToStringInclude
    protected final AtomicLong transferred = new AtomicLong();

    /** The size of segment for the read. */
    private int chunkSize;

    /** The unique input name to identify particular transfer part. */
    private String name;

    /**
     * The position from which the transfer will start. For the {@link File} it will be offset
     * where the transfer begin data transfer.
     */
    private long startPos;

    /** The total number of bytes to send. */
    private long cnt;

    /** Initialization completion flag. */
    private boolean inited;

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param chunkSize The size of chunk to read.
     * @param params Additional stream params.
     */
    protected AbstractChunkedStream(
        String name,
        long startPos,
        long cnt,
        int chunkSize,
        Map<String, Serializable> params
    ) {
        this.name = name;
        this.startPos = startPos;
        this.cnt = cnt;
        this.chunkSize = chunkSize;

        if (params != null)
            this.params.putAll(params);
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
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return Objects.requireNonNull(name);
    }

    /** {@inheritDoc} */
    @Override public int chunkSize() {
        return chunkSize;
    }

    /**
     * @param chunkSize The size of chunk in bytes.
     */
    protected void chunkSize(int chunkSize) {
        assert chunkSize > 0;

        this.chunkSize = chunkSize;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Serializable> params() {
        return new HashMap<>(params);
    }

    /**
     * @throws IOException If fails.
     */
    protected abstract void init() throws IOException;

    /** {@inheritDoc} */
    @Override public void setup(TransmitInputChannel in) throws IOException, IgniteCheckedException {
        TransmitMeta meta = new TransmitMeta();

        in.readMeta(meta);

        if (meta.initial()) {
            if (!inited) {
                name = meta.name();
                startPos = meta.offset();
                cnt = meta.count();
                params.putAll(meta.params());

                init();

                inited = true;
            }
            else
                throw new IgniteCheckedException("Attempt to read a new file from channel, but previous was not fully " +
                    "loaded [new=" + meta.name() + ", old=" + name() + ']');
        }
        else {
            if (inited) {
                if (!name().equals(meta.name()))
                    throw new IgniteCheckedException("Attempt to load different file name [name=" + name() +
                        ", meta=" + meta + ']');
                else if (startPosition() + transferred() != meta.offset())
                    throw new IgniteCheckedException("The next chunk input is incorrect " +
                        "[postition=" + startPosition() + ", transferred=" + transferred() + ", meta=" + meta + ']');
                else if (count() != meta.count())
                    throw new IgniteCheckedException(" The count of bytes to transfer for the next chunk is incorrect " +
                        "[count=" + count() + ", transferred=" + transferred() +
                        ", startPos=" + startPosition() + ", meta=" + meta + ']');
            }
            else
                throw new IgniteCheckedException("The setup of previous stream read failed [new=" + meta.name() +
                    ", old=" + name() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void setup(TransmitOutputChannel out) throws IOException {
        init();

        out.writeMeta(meta());
    }

    /** {@inheritDoc} */
    @Override public TransmitMeta meta() {
        return new TransmitMeta(name(),
            startPosition() + transferred(),
            count(),
            transferred() == 0,
            params());
    }

    /** {@inheritDoc} */
    @Override public void checkStreamEOF() throws IOException {
        if (transferred() < count()) {
            throw new IOException("Stream EOF occurred, but the file is not fully transferred " +
                "[count=" + count() + ", transferred=" + transferred() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public boolean endStream() {
        return transferred.get() == cnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractChunkedStream.class, this);
    }
}
