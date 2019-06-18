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

package org.apache.ignite.internal.managers.communication.transmit.channel;

import java.io.IOException;

/**
 * The class represents an exception on the transport level transmission (not the handler one). For instance,
 * case when the remote peer has closed the connection correctly, so read or write operation over given
 * socket channel will throw an {@link IOException} and it will be converted to this one exception.
 * <p>
 * Please, see {@link AbstractTransmitChannel} for details or such connection error handling.
 */
public class TransmitException extends IOException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs an {@code TransmitException} with the specified detail message.
     */
    public TransmitException(String msg) {
        super(msg);
    }

    /**
     * Constructs an {@code TransmitException} with the specified detail message and cause.
     */
    public TransmitException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
