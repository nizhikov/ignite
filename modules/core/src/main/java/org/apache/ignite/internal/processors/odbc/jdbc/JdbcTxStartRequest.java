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
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxStartRequest.ClientTransactionData;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * JDBC start transaction request.
 */
public class JdbcTxStartRequest extends JdbcRequest {
    /** */
    private ClientTransactionData data;

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

        this.data = new ClientTransactionData(concurrency, isolation, timeout, lb);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        data.write(writer);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        data = ClientTransactionData.read(reader);
    }

    /** */
    public ClientTransactionData data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcTxStartRequest.class, this);
    }
}
