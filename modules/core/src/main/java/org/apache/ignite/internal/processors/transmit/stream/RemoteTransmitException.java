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

package org.apache.ignite.internal.processors.transmit.stream;

import java.io.IOException;

/**
 *
 */
public class RemoteTransmitException extends IOException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs an {@code RemoteTransmitException} with the specified detail message.
     */
    public RemoteTransmitException(String msg) {
        super(msg);
    }

    /**
     * Constructs an {@code RemoteTransmitException} with the specified detail message and cause.
     */
    public RemoteTransmitException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
