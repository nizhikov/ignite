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

package org.apache.ignite.internal.client.thin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientDisconnectListener;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.client.thin.TcpClientTransactions.TcpClientTransaction;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.client.thin.ProtocolVersionFeature.EXPIRY_POLICY;
import static org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy.convertDuration;

/**
 * Implementation of {@link ClientCache} over TCP protocol.
 */
class TcpClientCache<K, V> implements ClientCache<K, V> {
    /** "Keep binary" flag mask. */
    private static final byte KEEP_BINARY_FLAG_MASK = 0x01;

    /** "Transactional" flag mask. */
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x02;

    /** "With expiry policy" flag mask. */
    private static final byte WITH_EXPIRY_POLICY_FLAG_MASK = 0x04;

    /** Platform type: Java platform. */
    static final byte JAVA_PLATFORM = 1;

    /** Cache id. */
    private final int cacheId;

    /** Channel. */
    private final ReliableChannel ch;

    /** Cache name. */
    private final String name;

    /** Marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Transactions facade. */
    private final TcpClientTransactions transactions;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /** Indicates if cache works with Ignite Binary format. */
    private final boolean keepBinary;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /** Cache entry listeners registry. */
    private final ClientCacheEntryListenersRegistry lsnrsRegistry;

    /** JCache adapter. */
    private final Cache<K, V> jCacheAdapter;

    /** Constructor. */
    TcpClientCache(String name, ReliableChannel ch, ClientBinaryMarshaller marsh, TcpClientTransactions transactions,
        ClientCacheEntryListenersRegistry lsnrsRegistry) {
        this(name, ch, marsh, transactions, lsnrsRegistry, false, null);
    }

    /** Constructor. */
    TcpClientCache(String name, ReliableChannel ch, ClientBinaryMarshaller marsh, TcpClientTransactions transactions,
        ClientCacheEntryListenersRegistry lsnrsRegistry, boolean keepBinary, ExpiryPolicy expiryPlc) {
        this.name = name;
        this.cacheId = ClientUtils.cacheId(name);
        this.ch = ch;
        this.marsh = marsh;
        this.transactions = transactions;
        this.lsnrsRegistry = lsnrsRegistry;

        serDes = new ClientUtils(marsh);

        this.keepBinary = keepBinary;
        this.expiryPlc = expiryPlc;

        jCacheAdapter = new ClientJCacheAdapter<>(this);
    }

    /** {@inheritDoc} */
    @Override public V get(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_GET,
            null,
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<V> getAsync(K key) {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_GET,
                null,
                this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_PUT,
            req -> writeObject(req, val),
            null
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> putAsync(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_PUT,
                req -> writeObject(req, val),
                null
        );
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_CONTAINS_KEY,
            null,
            res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> containsKeyAsync(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_CONTAINS_KEY,
                null,
                res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.isEmpty())
            return true;

        return ch.service(
            ClientOperation.CACHE_CONTAINS_KEYS,
            req -> writeKeys(keys, req),
            res -> res.in().readBoolean());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> containsKeysAsync(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.isEmpty())
            return IgniteClientFutureImpl.completedFuture(true);

        return ch.serviceAsync(
            ClientOperation.CACHE_CONTAINS_KEYS,
            req -> writeKeys(keys, req),
            res -> res.in().readBoolean());
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public ClientCacheConfiguration getConfiguration() throws ClientException {
        return ch.service(
            ClientOperation.CACHE_GET_CONFIGURATION,
            this::writeCacheInfo,
            this::getClientCacheConfiguration
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<ClientCacheConfiguration> getConfigurationAsync() throws ClientException {
        return ch.serviceAsync(
                ClientOperation.CACHE_GET_CONFIGURATION,
                this::writeCacheInfo,
                this::getClientCacheConfiguration
        );
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws ClientException {
        return ch.service(
            ClientOperation.CACHE_GET_SIZE,
            req -> {
                writeCacheInfo(req);
                ClientUtils.collection(peekModes, req.out(), (out, m) -> out.writeByte((byte)m.ordinal()));
            },
            res -> (int)res.in().readLong()
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws ClientException {
        return ch.serviceAsync(
                ClientOperation.CACHE_GET_SIZE,
                req -> {
                    writeCacheInfo(req);
                    ClientUtils.collection(peekModes, req.out(), (out, m) -> out.writeByte((byte)m.ordinal()));
                },
                res -> (int)res.in().readLong()
        );
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.isEmpty())
            return new HashMap<>();

        return ch.service(ClientOperation.CACHE_GET_ALL, req -> writeKeys(keys, req), this::readEntries);
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Map<K, V>> getAllAsync(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.isEmpty())
            return IgniteClientFutureImpl.completedFuture(new HashMap<>());

        return ch.serviceAsync(ClientOperation.CACHE_GET_ALL, req -> writeKeys(keys, req), this::readEntries);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) throws ClientException {
        if (map == null)
            throw new NullPointerException("map");

        if (map.isEmpty())
            return;

        ch.request(ClientOperation.CACHE_PUT_ALL, req -> writeEntries(map, req));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) throws ClientException {
        return ch.requestAsync(ClientOperation.CACHE_PUT_ALL, req -> writeEntries(map, req));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (oldVal == null)
            throw new NullPointerException("oldVal");

        if (newVal == null)
            throw new NullPointerException("newVal");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_REPLACE_IF_EQUALS,
            req -> {
                writeObject(req, oldVal);
                writeObject(req, newVal);
            },
            res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (oldVal == null)
            throw new NullPointerException("oldVal");

        if (newVal == null)
            throw new NullPointerException("newVal");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_REPLACE_IF_EQUALS,
                req -> {
                    writeObject(req, oldVal);
                    writeObject(req, newVal);
                },
                res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_REPLACE,
            req -> writeObject(req, val),
            res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> replaceAsync(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_REPLACE,
                req -> writeObject(req, val),
                res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_REMOVE_KEY,
            null,
            res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> removeAsync(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_REMOVE_KEY,
                null,
                res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (oldVal == null)
            throw new NullPointerException("oldVal");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_REMOVE_IF_EQUALS,
            req -> writeObject(req, oldVal),
            res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> removeAsync(K key, V oldVal) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (oldVal == null)
            throw new NullPointerException("oldVal");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_REMOVE_IF_EQUALS,
                req -> writeObject(req, oldVal),
                res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.isEmpty())
            return;

        ch.request(
            ClientOperation.CACHE_REMOVE_KEYS,
            req -> {
                writeKeys(keys, req);
            }
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> removeAllAsync(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.isEmpty())
            return IgniteClientFutureImpl.completedFuture(null);

        return ch.requestAsync(
                ClientOperation.CACHE_REMOVE_KEYS,
                req -> {
                    writeKeys(keys, req);
                }
        );
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws ClientException {
        ch.request(ClientOperation.CACHE_REMOVE_ALL, this::writeCacheInfo);
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> removeAllAsync() throws ClientException {
        return ch.requestAsync(ClientOperation.CACHE_REMOVE_ALL, this::writeCacheInfo);
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_GET_AND_PUT,
            req -> writeObject(req, val),
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<V> getAndPutAsync(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_GET_AND_PUT,
                req -> writeObject(req, val),
                this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_GET_AND_REMOVE,
            null,
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<V> getAndRemoveAsync(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_GET_AND_REMOVE,
                null,
                this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_GET_AND_REPLACE,
            req -> writeObject(req, val),
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<V> getAndReplaceAsync(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_GET_AND_REPLACE,
                req -> writeObject(req, val),
                this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_PUT_IF_ABSENT,
            req -> writeObject(req, val),
            res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> putIfAbsentAsync(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperationAsync(
                key,
                ClientOperation.CACHE_PUT_IF_ABSENT,
                req -> writeObject(req, val),
                res -> res.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_GET_AND_PUT_IF_ABSENT,
            req -> writeObject(req, val),
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<V> getAndPutIfAbsentAsync(K key, V val) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        if (val == null)
            throw new NullPointerException("val");

        return cacheSingleKeyOperationAsync(
            key,
            ClientOperation.CACHE_GET_AND_PUT_IF_ABSENT,
            req -> writeObject(req, val),
            this::readObject
        );
    }

    /** {@inheritDoc} */
    @Override public void clear() throws ClientException {
        ch.request(ClientOperation.CACHE_CLEAR, this::writeCacheInfo);
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> clearAsync() throws ClientException {
        return ch.requestAsync(ClientOperation.CACHE_CLEAR, this::writeCacheInfo);
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        cacheSingleKeyOperation(
            key,
            ClientOperation.CACHE_CLEAR_KEY,
            null,
            null
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> clearAsync(K key) throws ClientException {
        if (key == null)
            throw new NullPointerException("key");

        return cacheSingleKeyOperationAsync(
            key,
            ClientOperation.CACHE_CLEAR_KEY,
            null,
            null
        );
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.isEmpty())
            return;

        ch.request(
            ClientOperation.CACHE_CLEAR_KEYS,
            req -> writeKeys(keys, req)
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> clearAllAsync(Set<? extends K> keys) throws ClientException {
        if (keys == null)
            throw new NullPointerException("keys");

        if (keys.isEmpty())
            return IgniteClientFutureImpl.completedFuture(null);

        return ch.requestAsync(
            ClientOperation.CACHE_CLEAR_KEYS,
            req -> writeKeys(keys, req)
        );
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> ClientCache<K1, V1> withKeepBinary() {
        return keepBinary ? (ClientCache<K1, V1>)this :
            new TcpClientCache<>(name, ch, marsh, transactions, lsnrsRegistry, true, expiryPlc);
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> ClientCache<K1, V1> withExpirePolicy(ExpiryPolicy expirePlc) {
        return new TcpClientCache<>(name, ch, marsh, transactions, lsnrsRegistry, keepBinary, expirePlc);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        if (qry == null)
            throw new NullPointerException("qry");

        QueryCursor<R> res;

        if (qry instanceof ScanQuery)
            res = scanQuery((ScanQuery)qry);
        else if (qry instanceof SqlQuery)
            res = (QueryCursor<R>)sqlQuery((SqlQuery)qry);
        else if (qry instanceof SqlFieldsQuery)
            res = (QueryCursor<R>)query((SqlFieldsQuery)qry);
        else if (qry instanceof ContinuousQuery)
            res = query((ContinuousQuery<K, V>)qry, null);
        else
            throw new IllegalArgumentException(
                String.format("Query of type [%s] is not supported", qry.getClass().getSimpleName())
            );

        return res;
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        if (qry == null)
            throw new NullPointerException("qry");

        Consumer<PayloadOutputChannel> qryWriter = payloadCh -> {
            writeCacheInfo(payloadCh);
            serDes.write(qry, payloadCh.out());
        };

        return new ClientFieldsQueryCursor<>(new ClientFieldsQueryPager(
            ch,
            ClientOperation.QUERY_SQL_FIELDS,
            ClientOperation.QUERY_SQL_FIELDS_CURSOR_GET_PAGE,
            qryWriter,
            keepBinary,
            marsh
        ));
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(ContinuousQuery<K, V> qry, ClientDisconnectListener disconnectLsnr) {
        A.ensure(!(qry.getInitialQuery() instanceof ContinuousQuery), "Initial query for continuous query " +
            "can't be an instance of another continuous query");
        A.notNull(qry.getLocalListener(), "Local listener");
        A.ensure(!qry.isLocal(), "Local query is not supported by thin client");
        A.ensure(qry.isAutoUnsubscribe(), "AutoUnsubscribe flag is not supported by thin client");
        A.ensure(qry.getRemoteFilterFactory() == null || qry.getRemoteFilter() == null,
            "RemoteFilter and RemoteFilterFactory can't be used together");

        ClientCacheEntryListenerHandler<K, V> hnd = new ClientCacheEntryListenerHandler<>(
            jCacheAdapter,
            ch,
            marsh,
            keepBinary
        );

        hnd.startListen(
            qry.getLocalListener(),
            disconnectLsnr,
            qry.getRemoteFilterFactory() != null ? qry.getRemoteFilterFactory() : qry.getRemoteFilter() != null ?
                FactoryBuilder.factoryOf(qry.getRemoteFilter()) : null,
            qry.getPageSize(),
            qry.getTimeInterval(),
            qry.isIncludeExpired()
        );

        if (qry.getInitialQuery() != null) {
            try {
                QueryCursor<R> cur = (QueryCursor<R>)query(qry.getInitialQuery());

                return new ClientContinuousQueryCursor<>(cur, hnd);
            }
            catch (Exception e) {
                U.closeQuiet(hnd);

                throw e;
            }
        }
        else
            return new ClientContinuousQueryCursor<>(null, hnd);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cfg) {
        registerCacheEntryListener(cfg, null);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cfg,
        ClientDisconnectListener disconnectLsnr) {
        A.ensure(!cfg.isSynchronous(),
            "Unsupported cfg.isSynchronous() flag value");

        A.notNull(cfg.getCacheEntryListenerFactory(), "cfg.getCacheEntryListenerFactory()");

        ClientCacheEntryListenerHandler<K, V> hnd = new ClientCacheEntryListenerHandler<>(
            jCacheAdapter,
            ch,
            marsh,
            keepBinary
        );

        if (lsnrsRegistry.registerCacheEntryListener(name, cfg, hnd)) {
            CacheEntryListener<? super K, ? super V> locLsnr = cfg.getCacheEntryListenerFactory().create();

            ClientDisconnectListener disconnectLsnr0 = e -> {
                if (disconnectLsnr != null)
                    disconnectLsnr.onDisconnected(e);

                lsnrsRegistry.deregisterCacheEntryListener(name, cfg);
            };

            hnd.startListen(
                new ClientJCacheEntryListenerAdapter<>(locLsnr),
                disconnectLsnr0,
                cfg.getCacheEntryEventFilterFactory(),
                ContinuousQuery.DFLT_PAGE_SIZE,
                ContinuousQuery.DFLT_TIME_INTERVAL,
                locLsnr instanceof CacheEntryExpiredListener
            );
        }
        else
            throw new IllegalStateException("Listener is already registered for configuration: " + cfg);
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cfg) {
        ClientCacheEntryListenerHandler<?, ?> hnd = lsnrsRegistry.deregisterCacheEntryListener(name, cfg);

        U.closeQuiet(hnd);
    }

    /** Handle scan query. */
    private QueryCursor<Cache.Entry<K, V>> scanQuery(ScanQuery<K, V> qry) {
        Consumer<PayloadOutputChannel> qryWriter = payloadCh -> {
            writeCacheInfo(payloadCh);

            BinaryOutputStream out = payloadCh.out();

            if (qry.getFilter() == null)
                out.writeByte(GridBinaryMarshaller.NULL);
            else {
                serDes.writeObject(out, qry.getFilter());
                out.writeByte(JAVA_PLATFORM);
            }

            out.writeInt(qry.getPageSize());
            out.writeInt(qry.getPartition() == null ? -1 : qry.getPartition());
            out.writeBoolean(qry.isLocal());
        };

        return new ClientQueryCursor<>(new ClientQueryPager<>(
            ch,
            ClientOperation.QUERY_SCAN,
            ClientOperation.QUERY_SCAN_CURSOR_GET_PAGE,
            qryWriter,
            keepBinary,
            marsh,
            cacheId,
            qry.getPartition() == null ? -1 : qry.getPartition()
        ));
    }

    /** Handle SQL query. */
    private QueryCursor<Cache.Entry<K, V>> sqlQuery(SqlQuery qry) {
        Consumer<PayloadOutputChannel> qryWriter = payloadCh -> {
            writeCacheInfo(payloadCh);

            BinaryOutputStream out = payloadCh.out();

            serDes.writeObject(out, qry.getType());
            serDes.writeObject(out, qry.getSql());
            ClientUtils.collection(qry.getArgs(), out, serDes::writeObject);
            out.writeBoolean(qry.isDistributedJoins());
            out.writeBoolean(qry.isLocal());
            out.writeBoolean(qry.isReplicatedOnly());
            out.writeInt(qry.getPageSize());
            out.writeLong(qry.getTimeout());
        };

        return new ClientQueryCursor<>(new ClientQueryPager<>(
            ch,
            ClientOperation.QUERY_SQL,
            ClientOperation.QUERY_SQL_CURSOR_GET_PAGE,
            qryWriter,
            keepBinary,
            marsh
        ));
    }

    /**
     * Execute cache operation with a single key.
     */
    private <T> T cacheSingleKeyOperation(
        K key,
        ClientOperation op,
        Consumer<PayloadOutputChannel> additionalPayloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException {
        Consumer<PayloadOutputChannel> payloadWriter = req -> {
            writeCacheInfo(req);
            writeObject(req, key);

            if (additionalPayloadWriter != null)
                additionalPayloadWriter.accept(req);
        };

        // Transactional operation cannot be executed on affinity node, it should be executed on node started
        // the transaction.
        return transactions.tx() == null ? ch.affinityService(cacheId, key, op, payloadWriter, payloadReader) :
            ch.service(op, payloadWriter, payloadReader);
    }

    /**
     * Execute cache operation with a single key asynchronously.
     */
    private <T> IgniteClientFuture<T> cacheSingleKeyOperationAsync(
        K key,
        ClientOperation op,
        Consumer<PayloadOutputChannel> additionalPayloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException {
        Consumer<PayloadOutputChannel> payloadWriter = req -> {
            writeCacheInfo(req);
            writeObject(req, key);

            if (additionalPayloadWriter != null)
                additionalPayloadWriter.accept(req);
        };

        // Transactional operation cannot be executed on affinity node, it should be executed on node started
        // the transaction.
        return transactions.tx() == null
                ? ch.affinityServiceAsync(cacheId, key, op, payloadWriter, payloadReader)
                : ch.serviceAsync(op, payloadWriter, payloadReader);
    }

    /** Write cache ID and flags. */
    private void writeCacheInfo(PayloadOutputChannel payloadCh) {
        BinaryOutputStream out = payloadCh.out();

        out.writeInt(cacheId);

        byte flags = keepBinary ? KEEP_BINARY_FLAG_MASK : 0;

        TcpClientTransaction tx = transactions.tx();

        if (expiryPlc != null) {
            ProtocolContext protocolCtx = payloadCh.clientChannel().protocolCtx();

            if (!protocolCtx.isFeatureSupported(EXPIRY_POLICY)) {
                throw new ClientProtocolError(String.format("Expire policies are not supported by the server " +
                    "version %s, required version %s", protocolCtx.version(), EXPIRY_POLICY.verIntroduced()));
            }

            flags |= WITH_EXPIRY_POLICY_FLAG_MASK;
        }

        if (tx != null) {
            if (tx.clientChannel() != payloadCh.clientChannel()) {
                throw new ClientException("Transaction context has been lost due to connection errors. " +
                    "Cache operations are prohibited until current transaction closed.");
            }

            flags |= TRANSACTIONAL_FLAG_MASK;
        }

        out.writeByte(flags);

        if ((flags & WITH_EXPIRY_POLICY_FLAG_MASK) != 0) {
            out.writeLong(convertDuration(expiryPlc.getExpiryForCreation()));
            out.writeLong(convertDuration(expiryPlc.getExpiryForUpdate()));
            out.writeLong(convertDuration(expiryPlc.getExpiryForAccess()));
        }

        if ((flags & TRANSACTIONAL_FLAG_MASK) != 0)
            out.writeInt(tx.txId());
    }

    /** */
    private <T> T readObject(BinaryInputStream in) {
        return serDes.readObject(in, keepBinary);
    }

    /** */
    private <T> T readObject(PayloadInputChannel payloadCh) {
        return readObject(payloadCh.in());
    }

    /** */
    private void writeObject(PayloadOutputChannel payloadCh, Object obj) {
        serDes.writeObject(payloadCh.out(), obj);
    }

    /** */
    @Nullable private ClientCacheConfiguration getClientCacheConfiguration(PayloadInputChannel res) {
        try {
            return serDes.cacheConfiguration(res.in(), res.clientChannel().protocolCtx());
        }
        catch (IOException e) {
            return null;
        }
    }

    /** */
    private void writeKeys(Set<? extends K> keys, PayloadOutputChannel req) {
        writeCacheInfo(req);
        ClientUtils.collection(keys, req.out(), serDes::writeObject);
    }

    /** */
    private Map<K, V> readEntries(PayloadInputChannel res) {
        BinaryInputStream in = res.in();

        int cnt = in.readInt();
        Map<K, V> map = new HashMap<>();

        for (int i = 0; i < cnt; i++)
            map.put(readObject(in), readObject(in));

        return map;
    }

    /** */
    private void writeEntries(Map<? extends K, ? extends V> map, PayloadOutputChannel req) {
        writeCacheInfo(req);
        ClientUtils.collection(
                map.entrySet(),
                req.out(),
                (out, e) -> {
                    serDes.writeObject(out, e.getKey());
                    serDes.writeObject(out, e.getValue());
                });
    }
}
