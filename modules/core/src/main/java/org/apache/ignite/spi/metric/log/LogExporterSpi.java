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

package org.apache.ignite.spi.metric.log;

import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;

/**
 * This SPI implementation exports metrics to Ignite log.
 */
public class LogExporterSpi extends PushMetricsExporterAdapter {
    /** {@inheritDoc} */
    @Override public void export() {
        if (!log.isInfoEnabled()) {
            LT.warn(log, "LogExporterSpi configured but INFO level is disabled. " +
                "Metrics will not be export to the log! Please, check logger settings.");

            return;
        }

        log.info("Metrics:");

        mreg.forEach(grp -> {
            if (mregFilter != null && !mregFilter.test(grp))
                return;

            grp.forEach(m -> log.info(m.name() + " = " + m.getAsString()));
        });
    }
}
