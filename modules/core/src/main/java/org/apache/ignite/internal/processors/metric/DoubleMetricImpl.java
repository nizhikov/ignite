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

package org.apache.ignite.internal.processors.metric;

import java.util.function.DoubleSupplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.gauge.Gauge;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation based on primitive supplier.
 */
public class DoubleMetricImpl extends AbstractMetric implements DoubleMetric, Gauge {
    /** Value supplier. */
    private final DoubleSupplier val;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param name Name.
     * @param descr Description.
     * @param val Supplier.
     * @param log Logger.
     */
    public DoubleMetricImpl(String name, @Nullable String descr, DoubleSupplier val, IgniteLogger log) {
        super(name, descr);

        this.val = val;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public double value() {
        try {
            return val.getAsDouble();
        }
        catch (Exception e) {
            LT.warn(log, e, "Error on metric calculation [name=" + name() + ']', false, true);

            return 0;
        }
    }
}
