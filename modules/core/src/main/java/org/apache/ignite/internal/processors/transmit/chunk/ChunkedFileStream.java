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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.transmit.FileHandler;
import org.apache.ignite.internal.processors.transmit.channel.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.channel.TransmitOutputChannel;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ChunkedFileStream extends AbstractChunkedStream {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** */
    private final FileHandler handler;

    /** The destination object to transfer data to\from. */
    private String fileAbsPath;

    /** The abstract java representation of the chunked file. */
    private File file;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param handler The file handler to process download result.
     * @param chunkSize The size of chunk to read.
     */
    public ChunkedFileStream(
        FileHandler handler,
        String name,
        Long position,
        Long count,
        int chunkSize,
        Map<String, Serializable> params
    ) {
        super(name, position, count, chunkSize, params);

        this.handler = Objects.requireNonNull(handler);
    }

    /**
     * @param handler The file handler to process download result.
     * @param chunkSize The size of chunk to read.
     */
    public ChunkedFileStream(FileHandler handler, int chunkSize) {
        this(handler, null, null, null, chunkSize, null);
    }

    /**
     * Explicitly open the underlying file if not done yet.
     */
    public void open() throws IOException {
        if (fileIo == null) {
            fileIo = dfltIoFactory.create(Objects.requireNonNull(file));

            fileIo.position(startPosition());
        }
    }

    /** {@inheritDoc} */
    @Override public void transferred(long cnt) {
        super.transferred(cnt);

        try {
            if (fileIo != null)
                fileIo.position(startPosition() + cnt);
        }
        catch (IOException e) {
            throw new IgniteException("Unable to set new start file channel position [pos=" + (startPosition() + cnt) + ']');
        }
    }

    /** {@inheritDoc} */
    @Override protected void init() throws IOException {
        if (file == null) {
            fileAbsPath = handler.begin(name(), startPosition(), count(), params());

            file = new File(fileAbsPath);
        }
    }

    /** {@inheritDoc} */
    @Override public void readChunk(TransmitInputChannel channel) throws IOException {
        open();

        long batchSize = Math.min(chunkSize(), count() - transferred.get());

        long readed = channel.readInto(fileIo, startPosition() + transferred.get(), batchSize);

        if (readed > 0)
            transferred.addAndGet(readed);

        if (endStream())
            handler.end(file, params());
    }

    /** {@inheritDoc} */
    @Override public void writeChunk(TransmitOutputChannel channel) throws IOException {
        open();

        long batchSize = Math.min(chunkSize(), count() - transferred.longValue());

        long sent = channel.writeFrom(startPosition() + transferred.longValue(), batchSize, fileIo);

        if (sent > 0)
            transferred.addAndGet(sent);

        if (endStream())
            handler.end(file, params());
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(fileIo);

        fileIo = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkedFileStream.class, this, "super", super.toString());
    }
}
