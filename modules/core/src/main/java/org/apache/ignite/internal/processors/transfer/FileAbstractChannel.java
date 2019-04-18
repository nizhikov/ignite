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

package org.apache.ignite.internal.processors.transfer;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

/**
 *
 */
abstract class FileAbstractChannel implements AutoCloseable {
    /** */
    protected final IgniteLogger log;

    /** */
    protected final SocketChannel channel;

    /** If the channel is not been used anymore. */
    protected AtomicBoolean stopped = new AtomicBoolean();

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     * @throws IOException If fails.
     */
    protected FileAbstractChannel(
        GridKernalContext ktx,
        IgniteSocketChannel channel
    ) throws IOException {
        assert channel.config().blocking();

        this.channel = channel.channel();
        this.log = ktx.log(getClass());
    }

    /**
     * @param stopped The flag to set to.
     */
    void stopped(AtomicBoolean stopped) {
        this.stopped = stopped;
    }

    /**
     * @param file The file object to check.
     * @throws EOFException If the check fails.
     */
    public static void checkFileEOF(SegmentedFileIo file) throws EOFException {
        if (file.transferred() < file.count()) {
            throw new EOFException("The file expected to be fully transferred but didn't [count=" + file.count() +
                ", transferred=" + file.transferred() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(channel);
    }
}
