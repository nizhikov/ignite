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

import org.apache.ignite.IgniteCheckedException;

/**
 * The class represents an exception which handler is thrown on remote node. For instance,
 * remote handler can throw an `Not enough space` IOException. Sender will receive it on
 * reconnect and must stop the file transmisson.
 */
public class RemoteHandlerException extends IgniteCheckedException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs an {@code RemoteHandlerException} with the specified detail message.
     */
    public RemoteHandlerException(String msg) {
        super(msg);
    }

    /**
     * Constructs an {@code RemoteHandlerException} with the specified detail message and cause.
     */
    public RemoteHandlerException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
