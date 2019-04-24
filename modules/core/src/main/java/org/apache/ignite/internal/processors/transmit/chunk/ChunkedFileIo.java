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
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.transmit.stream.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.stream.TransmitOutputChannel;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ChunkedFileIo extends AbstractChunkedIo<File> {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param fileCfg The java {@link File} representation which should be transfered.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     */
    public ChunkedFileIo(File fileCfg, String name, long position, long count) {
        super(fileCfg, name, position, count);
    }

    /**
     * Explicitly open the underlying file if not done yet.
     */
    public void open() throws IOException {
        if (fileIo == null)
            fileIo = dfltIoFactory.create(obj);
    }

    /** {@inheritDoc} */
    @Override public void readChunk(TransmitInputChannel channel) throws IOException {
        open();

        long batchSize = Math.min(segmentSize, count - transferred.longValue());

        long readed = channel.readInto(fileIo, position + transferred.longValue(), batchSize);

        if (readed > 0)
            transferred.add(readed);
    }

    /** {@inheritDoc} */
    @Override public void writeChunk(TransmitOutputChannel channel) throws IOException {
        open();

        long batchSize = Math.min(segmentSize, count - transferred.longValue());

        long sent = channel.writeFrom(position + transferred.longValue(), batchSize, fileIo);

        if (sent > 0)
            transferred.add(sent);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        U.closeQuiet(fileIo);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkedFileIo.class, this);
    }
}
