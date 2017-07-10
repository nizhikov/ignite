package org.apache.ignite.cache.query;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.lang.IgniteClosure;

/**
 */
public final class ContinuousQueryWithTransformer<K, V, T> extends ContinuousQuery<K, V> {
    /** Remote transformer. */
    private IgniteClosure<Cache.Entry<K, V>, T> rmtTrans;

    /** Remote transformer factory. */
    private Factory<? extends IgniteClosure<Cache.Entry<K, V>, T>> rmtTransFactory;

    public ContinuousQueryWithTransformer<K, V, T> setRemoteTransformerFactory(
        Factory<? extends IgniteClosure<Cache.Entry<K, V>, T>> factory) {
        this.rmtTransFactory = factory;
        return this;
    }

    public ContinuousQueryWithTransformer<K, V, T> setRemoteTransformer(IgniteClosure<Cache.Entry<K, V>, T> rmtTrans) {
        this.rmtTrans = rmtTrans;
        return this;
    }

    public IgniteClosure<Cache.Entry<K, V>, T> getRemoteTransformer() {
        return rmtTrans;
    }

    public Factory<? extends IgniteClosure<Cache.Entry<K, V>, T>> getRemoteTransformerFactory() {
        return rmtTransFactory;
    }
}
