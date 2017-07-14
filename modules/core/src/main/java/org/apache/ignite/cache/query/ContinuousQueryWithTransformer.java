package org.apache.ignite.cache.query;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.lang.IgniteClosure;

/**
 */
public final class ContinuousQueryWithTransformer<K, V, T> extends BaseContinuousQuery<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Remote transformer factory. */
    private Factory<? extends IgniteClosure<Cache.Entry<K, V>, T>> rmtTransFactory;

    /** Local listener of transformed event */
    private TransformedEventListener<T> locTransEvtLsnr;

    public ContinuousQueryWithTransformer<K, V, T> setRemoteTransformerFactory(
        Factory<? extends IgniteClosure<Cache.Entry<K, V>, T>> factory) {
        this.rmtTransFactory = factory;
        return this;
    }

    public Factory<? extends IgniteClosure<Cache.Entry<K, V>, T>> getRemoteTransformerFactory() {
        return rmtTransFactory;
    }

    public ContinuousQueryWithTransformer<K, V, T> setLocalTransformedEventListener(
        TransformedEventListener<T> locTransEvtLsnr) {
        this.locTransEvtLsnr = locTransEvtLsnr;
        return this;
    }

    public TransformedEventListener<T> getLocalTransformedEventListener() {
        return locTransEvtLsnr;
    }

    public interface TransformedEventListener<T> {
        /**
         * Called after one or more entries have been updated.
         *
         * @param events The entries just updated transformed with #rmtTrans or #rmtTransFactory
         * @throws CacheEntryListenerException if there is problem executing the listener
         */
        void onUpdated(Iterable<? extends T> events) throws CacheEntryListenerException;
    }
}
