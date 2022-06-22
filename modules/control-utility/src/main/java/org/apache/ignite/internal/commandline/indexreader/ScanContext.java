/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.indexreader;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Traverse context, which is used for tree traversal and is unique for traversal of one single tree.
 */
class ScanContext {
    /** Cache id or {@code -1} for sequential scan. */
    final int cacheId;

    /** Page store. */
    final FilePageStore store;

    /** Page type statistics. */
    final Map<Class<? extends PageIO>, PagesStatistic> stats;

    /** Map of errors, pageId -> set of exceptions. */
    final Map<Long, List<String>> errors;

    /** List of items storage. */
    final ItemStorage items;

    /** */
    public ScanContext(int cacheId, FilePageStore store, ItemStorage items) {
        this.cacheId = cacheId;
        this.store = store;
        this.items = items;
        this.stats = new LinkedHashMap<>();
        this.errors = new LinkedHashMap<>();
    }

    /** */
    public void onPageIO(PageIO io, long addr) {
        onPageIO(io, stats, 1, io.getFreeSpace(store.getPageSize(), addr));
    }

    /** */
    public static void onPageIO(PageIO io, Map<Class<? extends PageIO>, PagesStatistic> stats, long cnt, int free) {
        PagesStatistic stat = stats.computeIfAbsent(io.getClass(), k -> new PagesStatistic());

        stat.cnt += cnt;
        stat.freeSpace += free;
    }

    /** */
    public static void merge(
        Class<? extends PageIO> io,
        Map<Class<? extends PageIO>, PagesStatistic> stats,
        PagesStatistic stat0
    ) {
        PagesStatistic stat = stats.computeIfAbsent(io, k -> new PagesStatistic());

        stat.cnt += stat0.cnt;
        stat.freeSpace += stat0.freeSpace;
    }

    /** */
    public void onLeafPage(long pageId, List<Object> data) {
        data.forEach(items::add);
    }

    /** */
    static class PagesStatistic {
        /** Count of pages. */
        long cnt;

        /** Summary free space. */
        long freeSpace;
    }
}
