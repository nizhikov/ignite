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

package org.apache.ignite.internal.management.meta;

import lombok.Data;
import org.apache.ignite.internal.management.api.BaseCommand;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.management.api.Parameter;

/**
 *
 */
@Data
public class MetaUpdateCommand extends BaseCommand implements ExperimentalCommand {
    /** */
    @Parameter(javaStyleExample = true, javaStyleName = true, example = "<fileName>")
    private String in;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Update cluster metadata from specified file (file name is required)";
    }
}
