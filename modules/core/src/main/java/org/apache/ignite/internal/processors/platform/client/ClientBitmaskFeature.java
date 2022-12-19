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

package org.apache.ignite.internal.processors.platform.client;

import java.util.EnumSet;
import org.apache.ignite.client.ClientServices;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.ThinProtocolFeature;
import org.apache.ignite.internal.client.thin.TcpClientCache;

/**
 * Defines supported features for thin client.
 */
public enum ClientBitmaskFeature implements ThinProtocolFeature {
    /** Feature for user attributes. */
    USER_ATTRIBUTES(0),

    /** Compute tasks (execute by task name). */
    EXECUTE_TASK_BY_NAME(1),

    /** Adds cluster states besides ACTIVE and INACTIVE. */
    CLUSTER_STATES(2),

    /** Client discovery. */
    CLUSTER_GROUP_GET_NODES_ENDPOINTS(3),

    /** Cluster groups. */
    CLUSTER_GROUPS(4),

    /** Service invocation. This flag is not necessary and exists for legacy reasons. */
    SERVICE_INVOKE(5),

    /** Feature for use default query timeout if the qry timeout isn't set explicitly. */
    DEFAULT_QRY_TIMEOUT(6),

    /** Additional SqlFieldsQuery properties: partitions, updateBatchSize. */
    QRY_PARTITIONS_BATCH_SIZE(7),

    /** Binary configuration retrieval. */
    BINARY_CONFIGURATION(8),

    /** Handle of {@link ClientServices#serviceDescriptors()}. */
    GET_SERVICE_DESCRIPTORS(9),

    /** Invoke service methods with caller context. */
    SERVICE_INVOKE_CALLCTX(10),

    /** Handle OP_HEARTBEAT and OP_GET_IDLE_TIMEOUT. */
    HEARTBEAT(11),

    /** Data replication operations: {@link TcpClientCache#putAllConflict}, {@link TcpClientCache#removeAllConflict}. */
    DATA_REPLICATION_OPERATIONS(12),

    /** Send all mappings to the client including non-default affinity functions. */
    ALL_AFFINITY_MAPPINGS(13),

    /** IndexQuery. */
    INDEX_QUERY(14),

    /**
     * Force deactivation flag.
     * @see org.apache.ignite.client.ClientCluster#state(ClusterState, boolean)
     */
    FORCE_DEACTIVATION_FLAG(15);

    /** */
    private static final EnumSet<ClientBitmaskFeature> ALL_FEATURES_AS_ENUM_SET =
        EnumSet.allOf(ClientBitmaskFeature.class);

    /** Feature id. */
    private final int featureId;

    /**
     * @param id Feature ID.
     */
    ClientBitmaskFeature(int id) {
        featureId = id;
    }

    /** {@inheritDoc} */
    @Override public int featureId() {
        return featureId;
    }

    /**
     * @param bytes Feature byte array.
     * @return Set of supported features.
     */
    public static EnumSet<ClientBitmaskFeature> enumSet(byte[] bytes) {
        return ThinProtocolFeature.enumSet(bytes, ClientBitmaskFeature.class);
    }

    /** */
    public static EnumSet<ClientBitmaskFeature> allFeaturesAsEnumSet() {
        return ALL_FEATURES_AS_ENUM_SET.clone();
    }
}
