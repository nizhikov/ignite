package org.apache.ignite.internal.processors.cache.query.continuous;

/**
 * @author SBT-Izhikov-NV
 */
public class CacheContinuousQueryWithTransformerConcurrentPartitionUpdateTest
    extends CacheContinuousQueryConcurrentPartitionUpdateTest {
    @Override public boolean isContinuousWithTransformer() {
        return true;
    }
}
