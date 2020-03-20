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

package org.apache.ignite.internal.visor.compute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Arguments for task {@link VisorComputeCancelSessionsTask}
 */
public class VisorComputeCancelSessionsTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Session IDs to cancel. */
    private Set<IgniteUuid> sesIds;

    /** {@code True} if job should be runed on all nodes. */
    private boolean execOnAllNodes;

    /**
     * Default constructor.
     */
    public VisorComputeCancelSessionsTaskArg() {
        // No-op.
    }

    /**
     * @param sesIds Session IDs to cancel.
     */
    public VisorComputeCancelSessionsTaskArg(Set<IgniteUuid> sesIds) {
        this.sesIds = sesIds;
    }

    /**
     * @param sesIds Session IDs to cancel.
     * @param execOnAllNodes Execute on all nodes.
     */
    public VisorComputeCancelSessionsTaskArg(Set<IgniteUuid> sesIds, boolean execOnAllNodes) {
        this.sesIds = sesIds;
        this.execOnAllNodes = execOnAllNodes;
    }

    /**
     * @return Session IDs to cancel.
     */
    public Set<IgniteUuid> getSessionIds() {
        return sesIds;
    }

    /** @return {@code True} if job should be runed on all nodes. */
    public boolean isExecOnAllNodes() {
        return execOnAllNodes;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, sesIds);
        out.writeBoolean(execOnAllNodes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        sesIds = U.readSet(in);
        execOnAllNodes = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorComputeCancelSessionsTaskArg.class, this);
    }
}
