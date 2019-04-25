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

package org.apache.ignite.internal.processors.monitoring.lists;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class MonitoringListImpl<Id, Data> implements MonitoringList<Id, Data> {
    private String name;

    private MonitoringGroup group;

    private Map<Id, ListRow<Id, Data>> rows = new HashMap<>();

    public MonitoringListImpl(MonitoringGroup group, String name) {
        this.name = name;
        this.group = group;
    }

    @Override public String name() {
        return name;
    }

    @Override public void add(Id id, String sessionId, Data data) {
        ListRow<Id, Data> old = rows.putIfAbsent(id, new ListRowImpl<>(id, sessionId, data));

        assert old == null;
    }

    @Override public void add(Id id, String sessionId, String name, Data data) {
        ListRow<Id, Data> old = rows.putIfAbsent(id, new NamedListRowImpl<>(id, sessionId, name, data));

        assert old == null;
    }

    @Override public void update(Id id, Data data) {
        if (!rows.containsKey(id))
            return; //TODO: should we throw an exception here?

        rows.get(id).data(data);
    }

    @Override public void remove(Id id) {
        rows.remove(id);
    }

    @NotNull @Override public Iterator<ListRow<Id, Data>> iterator() {
        return rows.values().iterator();
    }
}
