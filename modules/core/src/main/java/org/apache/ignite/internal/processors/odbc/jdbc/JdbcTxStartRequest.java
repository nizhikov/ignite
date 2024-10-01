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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * JDBC start transaction request.
 */
public class JdbcTxStartRequest extends JdbcRequest {
    /** Transaction concurrency control. */
    private TransactionConcurrency concurrency;

    /** Transaction isolation level. */
    private TransactionIsolation isolation;

    /** Transaction timeout. */
    private long timeout;

    /** Transaction label. */
    private String lb;

    /** Default constructor is used for deserialization. */
    public JdbcTxStartRequest() {
        super(TX_START);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Isolation level.
     * @param timeout Timeout.
     * @param lb Label.
     */
    public JdbcTxStartRequest(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        String lb
    ) {
        this();

        this.concurrency = concurrency;
        this.isolation = isolation;
        this.timeout = timeout;
        this.lb = lb;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeByte((byte)concurrency.ordinal());
        writer.writeByte((byte)isolation.ordinal());
        writer.writeLong(timeout);
        writer.writeString(lb);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        concurrency = TransactionConcurrency.fromOrdinal(reader.readByte());
        isolation = TransactionIsolation.fromOrdinal(reader.readByte());
        timeout = reader.readLong();
        lb = reader.readString();
    }

    /** */
    public TransactionConcurrency concurrency() {
        return concurrency;
    }

    /** */
    public TransactionIsolation isolation() {
        return isolation;
    }

    /** */
    public long timeout() {
        return timeout;
    }

    /** */
    public String label() {
        return lb;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcTxStartRequest.class, this);
    }
}
