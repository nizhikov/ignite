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

package org.apache.ignite.internal.management.api;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;

/**
 *
 */
public abstract class AbstractCommandInvoker<A extends IgniteDataTransferObject> implements AutoCloseable {
    /** Command to execute. */
    protected final Command<A, ?> cmd;

    /** Parsed argument. */
    protected final A arg;

    /** @param cmd Command to execute. */
    protected AbstractCommandInvoker(Command<A, ?> cmd, A arg) {
        this.cmd = cmd;
        this.arg = arg;
    }

    /**
     * Actual command execution with verbose mode if needed.
     * Implement it if your command supports verbose mode.
     *
     * @param printer Result printer.
     * @param verbose Use verbose mode or not
     * @return Result of operation (mostly usable for tests).
     * @throws GridClientException In case of error.
     */
    public <R> R invoke(Consumer<String> printer, boolean verbose) throws GridClientException {
        R res;

        if (cmd instanceof LocalCommand)
            res = ((LocalCommand<A, R>)cmd).execute(client(), arg, printer);
        else if (cmd instanceof ComputeCommand) {
            GridClientCompute compute = client().compute();

            Map<UUID, GridClientNode> nodes = compute.nodes().stream()
                .collect(toMap(GridClientNode::nodeId, n -> n));

            ComputeCommand<A, R> cmd = (ComputeCommand<A, R>)this.cmd;

            Collection<UUID> cmdNodes = cmd.nodes(nodes, arg);

            if (cmdNodes == null)
                cmdNodes = singleton(defaultNode().nodeId());

            for (UUID id : cmdNodes) {
                if (!nodes.containsKey(id))
                    throw new IllegalArgumentException("Node with id=" + id + " not found.");
            }

            Collection<GridClientNode> connectable = F.viewReadOnly(
                cmdNodes,
                nodes::get,
                id -> nodes.get(id).connectable()
            );

            if (!F.isEmpty(connectable))
                compute = compute.projection(connectable);

            res = compute.execute(cmd.taskClass().getName(), new VisorTaskArgument<>(cmdNodes, arg, false));

            cmd.printResult(arg, res, printer);
        }
        else
            throw new IllegalArgumentException("Unknown command type: " + cmd);

        return res;
    }

    /**
     * Method to create thin client for communication with cluster.
     *
     * @return Grid thin client instance which is already connected to cluster.
     * @throws GridClientException If error occur.
     */
    protected abstract GridClient client() throws GridClientException;

    /** @return Cluster nodes. */
    protected abstract Map<UUID, GridClientNode> nodes() throws GridClientException;

    /** @return Default node to execute commands. */
    protected abstract GridClientNode defaultNode() throws GridClientException;
}
