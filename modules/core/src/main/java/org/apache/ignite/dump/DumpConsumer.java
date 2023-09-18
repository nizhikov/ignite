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

package org.apache.ignite.dump;

import java.util.Iterator;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Consumer of {@link Dump}.
 * This consumer will receive all {@link DumpEntry} stored in cache dump during {@code IgniteDumpReader} application invocation.
 * The lifecycle of the consumer is the following:
 * <ul>
 *     <li>Start of the consumer {@link #start()}.</li>
 *     <li>Stop of the consumer {@link #stop()}.</li>
 * </ul>
 *
 */
@IgniteExperimental
public interface DumpConsumer {
    /**
     * Starts the consumer.
     */
    public void start();

    /**
     * Handles type mappings.
     * @param mappings Mappings iterator.
     */
    void onMappings(Iterator<TypeMapping> mappings);

    /**
     * Handles binary types.
     * @param types Binary types iterator.
     */
    void onTypes(Iterator<BinaryType> types);

    /**
     * Handles cache configs.
     * @param caches Stored cache data.
     */
    void onCacheConfigs(Iterator<StoredCacheData> caches);

    /**
     * Handles cache data.
     * @param data Cache data iterator.
     */
    void onData(Iterator<DumpEntry> data);

    /**
     * Stops the consumer.
     * This method can be invoked only after {@link #start()}.
     */
    public void stop();
}
