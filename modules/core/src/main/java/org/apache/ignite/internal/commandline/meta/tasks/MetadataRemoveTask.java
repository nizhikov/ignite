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

package org.apache.ignite.internal.commandline.meta.tasks;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_METADATA_OPS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/**
 * Task for remove specified binary type.
 */
@GridInternal
public class MetadataRemoveTask extends VisorMultiNodeTask<MetadataTypeArgs, MetadataMarshalled, MetadataMarshalled> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<MetadataTypeArgs, MetadataMarshalled> job(MetadataTypeArgs arg) {
        return new MetadataRemoveJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected MetadataMarshalled reduce0(List<ComputeJobResult> results) {
        if (results.size() != 1)
            throw new IgniteException("Invalid job results. Expected exactly 1 result, but was: " + results);

        if (results.get(0).getException() != null)
            throw results.get(0).getException();
        else
            return results.get(0).getData();
    }

    /**
     * Job for remove specified binary type from.
     */
    private static class MetadataRemoveJob extends VisorJob<MetadataTypeArgs, MetadataMarshalled> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Job result: metadata info for removed type (used for job continuation). */
        private transient MetadataMarshalled res;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected MetadataRemoveJob(@Nullable MetadataTypeArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            return systemPermissions(ADMIN_METADATA_OPS);
        }

        /** {@inheritDoc} */
        @Override protected MetadataMarshalled run(@Nullable MetadataTypeArgs arg) throws IgniteException {
            try {
                assert Objects.nonNull(arg);

                int typeId = arg.typeId(ignite.context());

                BinaryMetadata meta = ((CacheObjectBinaryProcessorImpl)ignite.context().cacheObjects())
                    .binaryMetadata(typeId);

                if (meta == null)
                    return new MetadataMarshalled(null, null);

                byte[] marshalled = U.marshal(ignite.context(), meta);

                res = new MetadataMarshalled(marshalled, meta);

                ignite.context().cacheObjects().removeType(typeId);

                return res;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
