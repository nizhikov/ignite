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

package org.apache.ignite.spi.metric.counter;

import java.util.concurrent.atomic.DoubleAccumulator;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Double counter.
 */
public class DoubleCounter extends AbstractMetric implements DoubleMetric, Counter {
    /** Value. */
    private final DoubleAccumulator val;

    /**
     * @param name Name.
     * @param descr Description.
     */
    public DoubleCounter(String name, @Nullable String descr) {
        super(name, descr);

        this.val = new DoubleAccumulator(Double::sum, 0d);
    }

    /**
     * Adds x to the counter.
     *
     * @param x Value to be added.
     */
    public void add(double x) {
        val.accumulate(x);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        val.reset();
    }

    /** {@inheritDoc} */
    @Override public double value() {
        return val.doubleValue();
    }
}
