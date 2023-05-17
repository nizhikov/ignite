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

package org.apache.ignite.internal.management.tracing;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.management.tracing.TracingConfigurationCommand.TracingConfigurationCommandArg;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.tracing.configuration.VisorTracingConfigurationTask;
import org.apache.ignite.internal.visor.tracing.configuration.VisorTracingConfigurationTaskResult;
import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** */
@CliPositionalSubcommands
public class TracingConfigurationCommand extends CommandRegistryImpl<TracingConfigurationCommandArg, VisorTracingConfigurationTaskResult>
    implements ComputeCommand<TracingConfigurationCommandArg, VisorTracingConfigurationTaskResult>,
    ExperimentalCommand<TracingConfigurationCommandArg, VisorTracingConfigurationTaskResult> {
    /** */
    public TracingConfigurationCommand() {
        super(
            new TracingConfigurationGetAllCommand(),
            new TracingConfigurationGetCommand(),
            new TracingConfigurationResetAllCommand(),
            new TracingConfigurationResetCommand(),
            new TracingConfigurationSetCommand()
        );
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Print tracing configuration";
    }

    /** {@inheritDoc} */
    @Override public Class<TracingConfigurationGetAllCommandArg> argClass() {
        return TracingConfigurationGetAllCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorTracingConfigurationTask> taskClass() {
        return VisorTracingConfigurationTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, T3<Boolean, Object, Long>> nodes, TracingConfigurationCommandArg arg) {
        return coordinatorOrNull(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        TracingConfigurationCommandArg arg,
        VisorTracingConfigurationTaskResult res,
        Consumer<String> printer
    ) {
        res.print(printer);
    }

    /** */
    public abstract static class TracingConfigurationCommandArg extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }

    /** */
    public static class TracingConfigurationResetAllCommandArg extends TracingConfigurationGetAllCommandArg {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class TracingConfigurationResetCommandArg extends TracingConfigurationGetCommandArg {
        /** */
        private static final long serialVersionUID = 0;
    }
}
