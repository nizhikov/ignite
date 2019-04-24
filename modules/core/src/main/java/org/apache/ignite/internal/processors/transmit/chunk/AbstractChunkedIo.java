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
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.processors.transmit.stream.RemoteTransmitException;
import org.apache.ignite.internal.processors.transmit.stream.TransmitAbstractChannel;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
abstract class AbstractChunkedIo<T> implements ChunkedIo<T> {
    /** The default region size to transfer data. */
    public static final int DFLT_SEGMENT_SIZE = 1024 * 1024;

    /** The destination object to transfer data into. */
    protected final T obj;

    /** The unique input name to identify particular transfer part.*/
    protected final String name;

    /** The position offest to start at. */
    protected final long position;

    /** The total number of bytes to send. */
    protected final long count;

    /** The size of segment for the read. */
    protected final int segmentSize;

    /** The number of bytes successfully transferred druring iteration. */
    protected final LongAdder transferred = new LongAdder();

    /**
     * @param obj The destination object to transfer into.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     * @param segmentSize The size of segmented the read.
     */
    protected AbstractChunkedIo(T obj, String name, long position, long count, int segmentSize) {
        assert position >= 0 : "The file position must be non-negative";
        assert count >= 0 : "The number of bytes to sent must be positive";

        this.obj = Objects.requireNonNull(obj);
        this.name = Objects.requireNonNull(name);
        this.position = position;
        this.count = count;
        this.segmentSize = segmentSize;
    }

    /**
     * @param obj The destination object to transfer into.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     */
    protected AbstractChunkedIo(T obj, String name, long position, long count) {
        this(obj, name, position, count, DFLT_SEGMENT_SIZE);
    }

    /** {@inheritDoc} */
    @Override public T chunk() {
        return obj;
    }

    /** {@inheritDoc} */
    @Override public long postition() {
        return position;
    }

    /** {@inheritDoc} */
    @Override public long transferred() {
        return transferred.longValue();
    }

    /** {@inheritDoc} */
    @Override public long count() {
        return count;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /**
     * @param io The file object to check.
     * @throws IOException If the check fails.
     */
    public void checkChunkedIoEOF(ChunkedIo<?> io) throws IOException {
        if (io.transferred() < io.count()) {
            throw new RemoteTransmitException("Socket channel EOF received, but the file is not fully transferred " +
                "[count=" + io.count() + ", transferred=" + io.transferred() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public boolean endOfTransmit() {
        return transferred.longValue() == count;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractChunkedIo.class, this);
    }
}
