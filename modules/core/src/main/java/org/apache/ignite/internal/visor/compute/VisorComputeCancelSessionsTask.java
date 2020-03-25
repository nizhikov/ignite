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

package org.apache.ignite.internal.visor.compute;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cancels given task session.
 */
@GridInternal
@GridVisorManagementTask
public class VisorComputeCancelSessionsTask extends VisorOneNodeTask<VisorComputeCancelSessionsTaskArg, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorComputeCancelSessionsJob job(VisorComputeCancelSessionsTaskArg arg) {
        return new VisorComputeCancelSessionsJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorComputeCancelSessionsTaskArg> arg) {
        return F.transform(ignite.cluster().nodes(), ClusterNode::id);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Boolean reduce0(List<ComputeJobResult> results) {
        for (ComputeJobResult res : results) {
            if (!res.isCancelled() && res.getData() != null && (Boolean)res.getData())
                return true;
        }

        return false;
    }

    /**
     * Job that cancel task.
     */
    private static class VisorComputeCancelSessionsJob extends VisorJob<VisorComputeCancelSessionsTaskArg, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Map with task sessions IDs to cancel.
         * @param debug Debug flag.
         */
        private VisorComputeCancelSessionsJob(VisorComputeCancelSessionsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Boolean run(VisorComputeCancelSessionsTaskArg arg) {
            Set<IgniteUuid> sesIds = arg.getSessionIds();

            if (sesIds == null || sesIds.isEmpty())
                return false;

            IgniteCompute compute = ignite.compute(ignite.cluster().forLocal());

            Map<IgniteUuid, ComputeTaskFuture<Object>> futs = compute.activeTaskFutures();

            boolean res = false;

            for (IgniteUuid sesId : sesIds) {
                ComputeTaskFuture<Object> fut = futs.get(sesId);

                if (fut == null)
                    continue;

                fut.cancel();

                res = true;
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeCancelSessionsJob.class, this);
        }
    }
}
