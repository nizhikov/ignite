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
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.transmit.FileHandler;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class represents a readable file chunked object which supports the zero-copy streaming algorithm,
 * see {@link FileChannel#transferFrom(java.nio.channels.ReadableByteChannel, long, long)} for details.
 */
public class InputChunkedFile extends InputChunkedObject {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** Handler to notify when a file has been processed. */
    private final FileHandler handler;

    /** The abstract java representation of the chunked file. */
    private File file;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param handler The file handler to process download result.
     */
    public InputChunkedFile(FileHandler handler) {
        super(null, -1, -1, null);

        assert handler != null;

        this.handler = handler;
    }

    /** {@inheritDoc} */
    @Override protected void init(int chunkSize) throws IOException {
        if (file != null)
            return;

        chunkSize(chunkSize);

        String fileAbsPath = handler.path(name(), params());

        if (fileAbsPath == null)
            throw new IOException("Requested for the chunked stream a file absolute path is incorrect: " + this);

        file = new File(fileAbsPath);
    }

    /** {@inheritDoc} */
    @Override public void readChunk(ReadableByteChannel ch) throws IOException {
        long batchSize = Math.min(chunkSize(), count() - transferred);

        long readed = fileIo.transferFrom(ch, startPosition() + transferred, batchSize);

        if (readed > 0)
            transferred += readed;
    }

    /** {@inheritDoc} */
    @Override public void doRead(
        ReadableByteChannel ch,
        int timeout,
        TimeUnit unit
    ) throws IOException, IgniteCheckedException, InterruptedException {
        if (fileIo == null) {
            if (file == null)
                throw new IOException("Chunked file instance is not initialized");

            fileIo = dfltIoFactory.create(file);

            fileIo.position(startPosition());
        }

        super.doRead(ch, timeout, unit);

        if (transferred == cnt)
            handler.acceptFile(file, startPosition(), count(), params());

        checkTransferLimitCount();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(fileIo);

        fileIo = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InputChunkedFile.class, this, "super", super.toString());
    }
}
