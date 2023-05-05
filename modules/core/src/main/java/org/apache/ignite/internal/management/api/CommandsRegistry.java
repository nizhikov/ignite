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

package org.apache.ignite.internal.management.api;

import java.util.Iterator;
import java.util.Map;

/**
 * Registry that knows all of its subcommands.
 */
public interface CommandsRegistry extends Command {
    /**
     * @param name Name of the command.
     * @return Command instance by name.
     */
    public Command<?, ?> command(String name);

    /** @return Commands iterator. */
    public Iterator<Map.Entry<String, Command<?, ?>>> commands();

    /** {@inheritDoc} */
    @Override public default String description() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public default Class argClass() {
        throw new UnsupportedOperationException();
    }
}
