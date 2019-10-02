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

package org.apache.ignite.internal.managers.systemview;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.NotNull;

/**
 * System view backed by {@code data} container.
 * Each instance of {@code containers} collections should provide a array of data.
 *
 * @see SystemView
 */
public class SystemViewArrayContainerAdapter<C, R, D> extends AbstractSystemView<R> {
    /** Collections of the data containers */
    private final Collection<C> containers;

    /** Function to extract collection of the data from container. */
    private final Function<C, D[]> dataExtractor;

    /** Row function. */
    private final BiFunction<C, D, R> rowFunc;

    /**
     * @param name Name.
     * @param desc Description.
     * @param rowCls Row class.
     * @param walker Walker.
     * @param containers Container of data.
     * @param dataExtractor Data extractor function.
     * @param rowFunc Row function.
     */
    public SystemViewArrayContainerAdapter(String name, String desc, Class<R> rowCls,
        SystemViewRowAttributeWalker<R> walker,
        Collection<C> containers,
        Function<C, D[]> dataExtractor,
        BiFunction<C, D, R> rowFunc) {
        super(name, desc, rowCls, walker);

        this.containers = containers;
        this.dataExtractor = dataExtractor;
        this.rowFunc = rowFunc;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int sz = 0;

        for (C c : containers)
            sz += dataExtractor.apply(c).length;

        return sz;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<R> iterator() {
        return F.concat(F.iterator(containers,
            c -> F.iterator(Arrays.asList(dataExtractor.apply(c)).iterator(),
                d -> rowFunc.apply(c, d), true), true));
    }
}
