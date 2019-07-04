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

package org.apache.ignite.internal.managers.communication.chunk;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.TransmitMeta;

import static org.apache.ignite.internal.util.IgniteUtils.assertParameter;

/**
 * Class represents a receiver of data which can be pulled from a channel by chunks of
 * predefined size. Closes when a transmission of represented object ends.
 */
public abstract class AbstractReceiver extends AbstractTransmission {
    /** Initialization completion flag. */
    private boolean inited;

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     * @param stopChecker Node stop or prcoess interrupt checker.
     */
    protected AbstractReceiver(
        String name,
        long startPos,
        long cnt,
        Map<String, Serializable> params,
        Supplier<Boolean> stopChecker
    ) {
        super(name, startPos, cnt, params, stopChecker);
    }

    /**
     * @param ch Input channel to read data from.
     * @throws IOException If an io exception occurred.
     * @throws IgniteCheckedException If some check failed.
     */
    public void receive(ReadableByteChannel ch, TransmitMeta meta, int chunkSize)
        throws IOException, IgniteCheckedException {
        assert meta != null;
        assert chunkSize > 0;

        if (meta.initial()) {
            if (inited) {
                throw new IgniteCheckedException("Attempt to read a new file from channel, but previous was not fully " +
                    "loaded [new=" + meta.name() + ", old=" + name() + ']');
            }

            init(chunkSize);

            inited = true;
        }
        else {
            assertParameter(inited, "The setup of previous stream read failed [new=" + meta.name() +
                ", old=" + name() + ']');

            assertParameter(name().equals(meta.name()), "Attempt to load different file name [name=" + name() +
                ", meta=" + meta + ']');

            assertParameter(startPosition() + transferred() == meta.offset(), "The next chunk input is incorrect " +
                "[postition=" + startPosition() + ", transferred=" + transferred() + ", meta=" + meta + ']');

            assertParameter(count() == meta.count(), "The count of bytes to transfer for the next chunk is incorrect " +
                "[count=" + count() + ", transferred=" + transferred() +
                ", startPos=" + startPosition() + ", meta=" + meta + ']');
        }

        // Read data from the input.
        while (hasNextChunk()) {
            if (Thread.currentThread().isInterrupted() || stopped()) {
                throw new IgniteCheckedException("Thread has been interrupted or operation has been cancelled " +
                    "due to node is stopping. Channel processing has been stopped.");
            }

            readChunk(ch);
        }
    }

    /**
     * @param chunkSize The size of chunk to read.
     * @throws IgniteCheckedException If fails.
     */
    protected abstract void init(int chunkSize) throws IgniteCheckedException;

    /**
     * @param ch Channel to read data from.
     * @throws IOException If fails.
     * @throws IgniteCheckedException If fails.
     */
    protected abstract void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException;
}
