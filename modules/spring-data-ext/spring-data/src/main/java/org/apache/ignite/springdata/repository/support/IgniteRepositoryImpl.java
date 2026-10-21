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
package org.apache.ignite.springdata.repository.support;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.console.agent.service.CacheAgentService;
import org.apache.ignite.console.agent.service.ServiceResult;
import org.apache.ignite.springdata.proxy.IgniteCacheProxy;
import org.apache.ignite.springdata.proxy.IgniteClientCacheProxy;
import org.apache.ignite.springdata.proxy.IgniteClientProxy;
import org.apache.ignite.springdata.proxy.IgniteNodeCacheProxy;
import org.apache.ignite.springdata.proxy.IgniteNodeProxy;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.domain.Pageable;
import org.springframework.util.Assert;

/**
 * General Apache Ignite repository implementation. This bean should've never been loaded by context directly, only via
 * {@link IgniteRepositoryFactory}
 *
 * @param <V> the cache value type
 * @param <K> the cache key type
 * @author Apache Ignite Team
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 */
@Conditional(ConditionFalse.class)
public class IgniteRepositoryImpl<V, K extends Serializable> implements IgniteRepository<V, K> {
    /** Error message indicating that operation is supported only if {@link Ignite} instance is used to access the cluster. */
    private static final String UNSUPPORTED_ERR_MSG = "Current operation is supported only if Ignite node instance is" +
        " used to access the Ignite cluster. See " + RepositoryConfig.class.getName() + "#igniteInstance.";

    /** Name of the cluster service used to run full-text/scan queries when a thin client is used (TextQuery is not
     * supported by the thin client protocol). */
    private static final String CACHE_QUERY_DATA_SERVICE = "CacheQueryDataService";

    /**
     * Ignite Cache bound to the repository
     */
    private final IgniteCacheProxy<K, V> cache;

    /**
     * Ignite instance bound to the repository
     */
    private final IgniteProxy ignite;

    /** Domain (value) type bound to the repository, or {@code null} if it could not be determined. */
    private final Class<?> domainType;

    /** SQL type name of the domain type used by {@link TextQuery} and the query service, or {@code null}. */
    private final String typeName;


    /**
     * Repository constructor.
     *
     * @param ignite the ignite
     * @param cache  Initialized cache instance.
     */
    public IgniteRepositoryImpl(IgniteProxy ignite, IgniteCacheProxy<K, V> cache) {
        this(ignite, cache, null);
    }

    /**
     * Repository constructor.
     *
     * @param ignite    the ignite
     * @param cache     Initialized cache instance.
     * @param domainType Domain (value) type bound to the repository, used to resolve text-index type names.
     */
    public IgniteRepositoryImpl(IgniteProxy ignite, IgniteCacheProxy<K, V> cache, @Nullable Class<?> domainType) {
        this.cache = cache;
        this.ignite = ignite;
        this.domainType = domainType;
        this.typeName = domainType == null ? null : domainType.getSimpleName();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> cache() {
        if (cache instanceof IgniteNodeCacheProxy)
            return ((IgniteNodeCacheProxy<K, V>)cache).delegate();

        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public Ignite ignite() {
        if (ignite instanceof IgniteNodeProxy)
            return ((IgniteNodeProxy)ignite).delegate();

        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public <S extends V> S save(K key, S entity) {
        cache.put(key, entity);

        return entity;
    }

    /** {@inheritDoc} */
    @Override public <S extends V> Iterable<S> save(Map<K, S> entities) {
        cache.putAll(entities);

        return entities.values();
    }

    /** {@inheritDoc} */
    @Override public <S extends V> S save(K key, S entity, @Nullable ExpiryPolicy expiryPlc) {
        if (expiryPlc != null)
            cache.withExpiryPolicy(expiryPlc).put(key, entity);
        else
            cache.put(key, entity);
        return entity;
    }

    /** {@inheritDoc} */
    @Override public <S extends V> Iterable<S> save(Map<K, S> entities, @Nullable ExpiryPolicy expiryPlc) {
        if (expiryPlc != null)
            cache.withExpiryPolicy(expiryPlc).putAll(entities);
        else
            cache.putAll(entities);
        return entities.values();
    }

    /**
     * Not implemented.
     */
    @Override public <S extends V> S save(S entity) {
        if(IdExtractor.isEnable()) {
            Object key = IdExtractor.getId(entity);
            if (key != null) {
                cache.put((K) key, entity);
                return entity;
            } else {
                throw new IllegalArgumentException("Entity Id is null!");
            }
        }
        throw new UnsupportedOperationException("Use IgniteRepository.save(key,value) method instead.");
    }

    /**
     * Not implemented.
     */
    @Override public <S extends V> Iterable<S> saveAll(Iterable<S> entities) {
        if(IdExtractor.isEnable()) {
            Map<K,S> kv = new TreeMap<>();
            for (S entity : entities) {
                Object key = IdExtractor.getId(entity);
                if (key != null) {
                    kv.put((K) key, entity);
                }
                else{
                    throw new IllegalArgumentException("Entity Id is null!");
                }
            }
            cache.putAll(kv);
            return entities;
        }
        throw new UnsupportedOperationException("Use IgniteRepository.save(Map<keys,value>) method instead.");
    }

    /** {@inheritDoc} */
    @Override public Optional<V> findById(K id) {
        return Optional.ofNullable(cache.get(id));
    }

    /** {@inheritDoc} */
    @Override public boolean existsById(K id) {
        return cache.containsKey(id);
    }

    /** {@inheritDoc} */
    @Override public Iterable<V> findAll() {
        final Iterator<Cache.Entry<K, V>> iter = cache.iterator();

        return new Iterable<V>() {
            /** */
            @Override public Iterator<V> iterator() {
                return new Iterator<V>() {
                    /** {@inheritDoc} */
                    @Override public boolean hasNext() {
                        return iter.hasNext();
                    }

                    /** {@inheritDoc} */
                    @Override public V next() {
                        return iter.next().getValue();
                    }

                    /** {@inheritDoc} */
                    @Override public void remove() {
                        iter.remove();
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Iterable<V> findAllById(Iterable<K> ids) {
        if (ids instanceof Set)
            return cache.getAll((Set<K>)ids).values();

        if (ids instanceof Collection)
            return cache.getAll(new HashSet<>((Collection<K>)ids)).values();

        TreeSet<K> keys = new TreeSet<>();

        for (K id : ids)
            keys.add(id);

        return cache.getAll(keys).values();
    }

    /** {@inheritDoc} */
    @Override public long count() {
        return cache.size(CachePeekMode.PRIMARY);
    }

    /** {@inheritDoc} */
    @Override public void deleteById(K id) {
        cache.remove(id);
    }

    /** {@inheritDoc} */
    @Override public void delete(V entity) {
        if(IdExtractor.isEnable()) {
            Object key = IdExtractor.getId(entity);
            if(key!=null) {
                cache.remove((K)key);
                return;
            }
            else{
                throw new IllegalArgumentException("Entity Id is null!");
            }
        }
        throw new UnsupportedOperationException("Use IgniteRepository.deleteById(key) method instead.");
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Iterable<? extends V> entities) {
        TreeSet<K> keys = new TreeSet<>();
        if(IdExtractor.isEnable()) {
            for (V entity : entities) {
                Object key = IdExtractor.getId(entity);
                if (key != null) {
                    keys.add((K) key);
                }
                else{
                    throw new IllegalArgumentException("Entity Id is null!");
                }
            }
            cache.removeAll(keys);
            return;
        }
        throw new UnsupportedOperationException("Use IgniteRepository.deleteAllById(keys) method instead.");
    }

    /** {@inheritDoc} */
    @Override public void deleteAllById(Iterable<? extends K> ids) {
        if (ids instanceof Set) {
            cache.removeAll((Set<K>)ids);
            return;
        }

        if (ids instanceof Collection) {
            cache.removeAll(new HashSet<>((Collection<K>)ids));
            return;
        }

        TreeSet<K> keys = new TreeSet<>();

        for (K id : ids)
            keys.add(id);

        cache.removeAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void deleteAll() {
        cache.clear();
    }

    /** {@inheritDoc} */
    @Override public Iterable<V> findAllByText(@Nullable String query, Pageable pageable) {
        Assert.notNull(pageable, "pageable must not be null");

        if (typeName == null) {
            throw new IllegalStateException("Domain type is unknown for this repository. " +
                "findAllByText requires the repository to be created via " + IgniteRepositoryFactory.class.getName()
                + ".");
        }

        long offset = pageable.isPaged() ? pageable.getOffset() : 0L;
        int limit = pageable.isPaged() ? pageable.getPageSize() : Integer.MAX_VALUE;

        if (cache instanceof IgniteClientCacheProxy)
            return findAllByTextThinClient(query, offset, limit);

        return findAllByTextNode(query, offset, limit);
    }

    /**
     * Runs a {@link TextQuery} (or a {@link ScanQuery} when the query is empty) directly on the Ignite node and
     * applies the requested offset/limit.
     *
     * @param query  Full-text keyword, or {@code null}/blank to scan all entities.
     * @param offset Number of matching entries to skip.
     * @param limit  Maximum number of entities to return.
     * @return Matching entities.
     */
    private Iterable<V> findAllByTextNode(@Nullable String query, long offset, int limit) {
        Query<Cache.Entry<K, V>> iQry = isBlank(query)
            ? new ScanQuery<K, V>()
            : new TextQuery<K, V>(typeName, query);

        QueryCursor<Cache.Entry<K, V>> cursor = cache.query(iQry);

        try {
            List<V> res = new ArrayList<>();

            Iterator<Cache.Entry<K, V>> it = cursor.iterator();

            for (long i = 0; i < offset && it.hasNext(); i++)
                it.next();

            while (res.size() < limit && it.hasNext())
                res.add(it.next().getValue());

            return res;
        }
        finally {
            cursor.close();
        }
    }

    /**
     * Runs the full-text (or scan, when the query is empty) search through the {@code CacheQueryDataService} cluster
     * service, because the thin client does not support {@link TextQuery}. The returned service payload is parsed
     * and the requested offset/limit are applied.
     *
     * @param query  Full-text keyword, or {@code null}/blank to scan all entities.
     * @param offset Number of matching entries to skip.
     * @param limit  Maximum number of entities to return.
     * @return Matching entities.
     */
    private Iterable<V> findAllByTextThinClient(@Nullable String query, long offset, int limit) {
        IgniteClient cli = ((IgniteClientProxy)ignite).delegate();

        // Ask the service for offset + limit entries so the offset can be applied client side.
        long requested = (limit == Integer.MAX_VALUE) ? Integer.MAX_VALUE : offset + limit;

        Map<String, Object> payload = new HashMap<>();
        payload.put("type", typeName);
        payload.put("qry", isBlank(query) ? "" : query);
        payload.put("queryType", "SCAN");
        payload.put("pageSize", (int)Math.min(requested, Integer.MAX_VALUE));

        CacheAgentService svc = cli.services().serviceProxy(CACHE_QUERY_DATA_SERVICE, CacheAgentService.class);

        ServiceResult result = svc.call(cache.getName(), payload);

        return parseServiceResult(result, offset, limit);
    }

    /**
     * Parses the {@code CacheQueryDataService} response into domain entities, honoring offset/limit.
     * <p>
     * The service returns a {@link ServiceResult} whose {@code data} entry holds the query result serialized as JSON
     * in the {@code CacheQueryResult} shape: {@code {"items":[[key,value],...],"last":...,"queryId":...}}.
     *
     * @param result Service response.
     * @param offset Number of matching entries to skip.
     * @param limit  Maximum number of entities to return.
     * @return Matching entities.
     */
    @SuppressWarnings("unchecked")
    private Iterable<V> parseServiceResult(@Nullable ServiceResult result, long offset, int limit) {
        if (result == null)
            return Collections.emptyList();

        if (!result.isAcknowledged() || result.getErrorType() != null) {
            throw new IllegalStateException("CacheQueryDataService query failed [errorType=" + result.getErrorType()
                + ", messages=" + result.getMessages() + "]");
        }

        Map<String, Object> res = result.getResult();

        Object data = res == null ? null : res.get("data");

        if (data == null)
            return Collections.emptyList();

        ObjectMapper mapper = createJsonMapper();

        try {
            JsonNode root = mapper.readTree(data.toString());
            JsonNode items = root == null ? null : root.get("items");

            List<V> values = new ArrayList<>();

            if (items == null || !items.isArray())
                return values;

            long skipped = 0;

            for (JsonNode item : items) {
                if (skipped < offset) {
                    skipped++;
                    continue;
                }

                if (values.size() >= limit)
                    break;

                // Each item is a [key, value] pair; keep the value.
                JsonNode valNode = (item.isArray() && item.size() > 1) ? item.get(1) : item;

                values.add((V)mapper.convertValue(valNode, domainType));
            }

            return values;
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to parse CacheQueryDataService response.", e);
        }
    }

    /**
     * @return Jackson mapper able to resolve the {@code @class} type hints emitted by the Ignite REST mapper.
     */
    private static ObjectMapper createJsonMapper() {
        ObjectMapper mapper = new ObjectMapper();

        mapper.activateDefaultTypingAsProperty(
            LaissezFaireSubTypeValidator.instance,
            ObjectMapper.DefaultTyping.NON_FINAL,
            "@class");

        return mapper;
    }

    /**
     * @param str String to check.
     * @return {@code true} if the string is {@code null} or blank.
     */
    private static boolean isBlank(@Nullable String str) {
        return str == null || str.trim().isEmpty();
    }
}
