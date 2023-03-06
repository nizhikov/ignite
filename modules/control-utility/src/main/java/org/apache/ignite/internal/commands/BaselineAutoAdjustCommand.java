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

package org.apache.ignite.internal.commands;

import lombok.Data;
import org.apache.ignite.internal.commands.api.Command;
import org.apache.ignite.internal.commands.api.Parameter;
import org.apache.ignite.internal.commands.api.PositionalParameter;

/**
 *
 */
@Data
public class BaselineAutoAdjustCommand implements Command {
    /** */
    @PositionalParameter(optional = true)
    private Enabled enabled;

    /** */
    @Parameter(optional = true, withoutPrefix = true, example = "<timeoutMillis>")
    private long timeout;

    /** */
    @Parameter(optional = true)
    private Boolean yes;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Set baseline autoadjustment settings";
    }

    /** */
    public enum Enabled {
        /** */
        disable,

        /** */
        enable
    }
}
