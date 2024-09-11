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

/**
 * JDBC commit transaction request.
 */
public class JdbcTxEndRequest extends JdbcRequest {
    /** Transaction id. */
    private int txId;

    /** Transaction committed. */
    private boolean committed;

    /** Default constructor is used for deserialization. */
    public JdbcTxEndRequest() {
        super(TX_END);
    }

    /** {@inheritDoc} */
    protected JdbcTxEndRequest(int txId, boolean committed) {
        this();

        this.txId = txId;
        this.committed = committed;
    }

    /** @return Transaction id. */
    public int txId() {
        return txId;
    }

    /** @return {@code true} if transaction was committed on client, {@code false} otherwise. */
    public boolean committed() {
        return committed;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeInt(txId);
        writer.writeBoolean(committed);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        txId = reader.readInt();
        committed = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcTxEndRequest.class, this);
    }
}
