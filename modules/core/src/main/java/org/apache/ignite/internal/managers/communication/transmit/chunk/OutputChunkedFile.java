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
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.communication.transmit.channel.OutputTransmitChannel;
import org.apache.ignite.internal.managers.communication.transmit.channel.TransmitMeta;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class represents a writable file chunked object which supports the zero-copy streaming algorithm,
 * see {@link FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)} for details.
 */
public class OutputChunkedFile extends AbstractChunkedObject {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** The abstract java representation of the chunked file. */
    private File file;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param file File representation of current object.
     * @param pos File offset.
     * @param cnt Number of bytes to transfer.
     * @param chunkSize Size of each chunk.
     * @param params Additional file params.
     */
    public OutputChunkedFile(
        File file,
        long pos,
        long cnt,
        int chunkSize,
        Map<String, Serializable> params
    ) {
        super(file.getName(), pos, cnt, chunkSize, params);

        this.file = file;
    }

    /** {@inheritDoc} */
    @Override public void transferred(long cnt) {
        try {
            if (fileIo != null)
                fileIo.position(startPosition() + cnt);

            super.transferred(cnt);
        }
        catch (IOException e) {
            throw new IgniteException("Unable to set new start file channel position [pos=" + (startPosition() + cnt) + ']');
        }
    }

    /**
     * @param out The channel to write data to.
     * @throws IOException If failed.
     */
    public void setup(OutputTransmitChannel out) throws IOException {
        out.writeMeta(new TransmitMeta(name(),
            startPosition() + transferred(),
            count(),
            transferred() == 0,
            params(),
            null));
    }

    /**
     * @param out Channel to write data into.
     * @throws IOException If fails.
     */
    public void writeChunk(OutputTransmitChannel out) throws IOException {
        if (fileIo == null) {
            fileIo = dfltIoFactory.create(Objects.requireNonNull(file));

            fileIo.position(startPosition());
        }

        long batchSize = Math.min(chunkSize(), count() - transferred.longValue());

        long sent = out.writeFrom(startPosition() + transferred.longValue(), batchSize, fileIo);

        if (sent > 0)
            transferred.addAndGet(sent);

        checkExpectedBytesCount();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(fileIo);

        fileIo = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OutputChunkedFile.class, this, "super", super.toString());
    }
}
