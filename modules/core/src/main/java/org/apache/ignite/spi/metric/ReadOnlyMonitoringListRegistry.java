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

package org.apache.ignite.spi.metric;

import java.util.function.Consumer;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.spi.metric.list.MonitoringList;

/**
 * Read only monitoring list registry.
 *
 * @see GridMetricManager
 * @see MonitoringList
 * @see MetricExporterSpi
 */
public interface ReadOnlyMonitoringListRegistry extends Iterable<MonitoringList<?, ?>> {
    /**
     * Adds listener of list creation events.
     *
     * @param lsnr Listener.
     */
    public void addListCreationListener(Consumer<MonitoringList<?, ?>> lsnr);

    /**
     * Adds listener of list remove events.
     *
     * @param lsnr Listener.
     */
    public void addListRemoveListener(Consumer<MonitoringList<?, ?>> lsnr);
}
