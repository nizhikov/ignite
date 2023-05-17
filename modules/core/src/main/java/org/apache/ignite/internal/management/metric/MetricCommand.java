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

package org.apache.ignite.internal.management.metric;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.metric.VisorMetricTask;
import static java.util.Arrays.asList;
import static org.apache.ignite.internal.management.SystemViewCommand.printTable;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/** */
public class MetricCommand extends CommandRegistryImpl<MetricCommandArg, Map<String, ?>>
    implements ComputeCommand<MetricCommandArg, Map<String, ?>> {
    /** */
    public MetricCommand() {
        super(
            new MetricConfigureHistogramCommand(),
            new MetricConfigureHitrateCommand()
        );
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Print metric value";
    }

    /** {@inheritDoc} */
    @Override public Class<MetricCommandArg> argClass() {
        return MetricCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorMetricTask> taskClass() {
        return VisorMetricTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, T3<Boolean, Object, Long>> nodes, MetricCommandArg arg) {
        return arg.nodeId() == null ? null : Collections.singleton(arg.nodeId());
    }

    /** {@inheritDoc} */
    @Override public void printResult(MetricCommandArg arg, Map<String, ?> res, Consumer<String> printer) {
        if (res != null) {
            List<List<?>> data = res.entrySet().stream()
                .map(entry -> Arrays.asList(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

            printTable(asList("metric", "value"), asList(STRING, STRING), data, printer);
        }
        else
            printer.accept("No metric with specified name was found [name=" + arg.name() + "]");
    }
}
