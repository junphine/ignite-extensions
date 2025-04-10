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

package org.apache.ignite.cache.store.bson;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeDefaultHasher;
import org.apache.ignite.cache.store.jdbc.JdbcTypeHasher;
import org.apache.ignite.cache.store.jdbc.JdbcTypesDefaultTransformer;
import org.apache.ignite.cache.store.jdbc.JdbcTypesTransformer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.SpringApplicationContextResource;

/**
 * {@link Factory} implementation for {@link CacheJdbcPojoStore}.
 *
 * Use this factory to pass {@link CacheJdbcPojoStore} to {@link CacheConfiguration}.
 * <p>
 * Please note, that {@link DocumentLoadOnlyStoreFactory#setDataSource(DataSource)} is deprecated and
 * {@link DocumentLoadOnlyStoreFactory#setDataSourceFactory(Factory)} or
 * {@link DocumentLoadOnlyStoreFactory#setDataSourceBean(String)} should be used instead.
 * 
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class DocumentLoadOnlyStoreFactory<K> implements Factory<DocumentLoadOnlyStore<K>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default value for write attempts. */
    public static final int DFLT_WRITE_ATTEMPTS = 2;

    /** Default batch size for put and remove operations. */
    public static final int DFLT_BATCH_SIZE = 512;

    /** Default batch size for put and remove operations. */
    public static final int DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD = 512;

    /** Maximum batch size for writeAll and deleteAll operations. */
    private int batchSize = DFLT_BATCH_SIZE;

    /** Name of data source bean. */
    private String dataSrc;

    private String idField = "_id";
    
    private boolean streamerEnabled = false;


	/** Max workers thread count. These threads are responsible for load cache. */
    private int maxPoolSize = Runtime.getRuntime().availableProcessors();


    /** Parallel load cache minimum threshold. If {@code 0} then load sequentially. */
    private int parallelLoadCacheMinThreshold = DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD;
    
    /** Types that store could process. */
    private JdbcType[] types;    


    /** Application context. */
    @SpringApplicationContextResource
    private transient Object appCtx;

    /** {@inheritDoc} */
    @Override public DocumentLoadOnlyStore<K> create() {
    	DocumentLoadOnlyStore<K> store = new DocumentLoadOnlyStore<>(dataSrc,idField);
        store.setBatchSize(batchSize);
        store.setTypes(types);
        store.streamerEnabled = this.streamerEnabled;
        return store;
    }

    /**
     * Sets data source. Data source should be fully configured and ready-to-use.
     *
     * @param dataSrc Data source.
     * @return {@code This} for chaining.
     * @see CacheJdbcPojoStore#setDataSource(DataSource)
     */
    public DocumentLoadOnlyStoreFactory<K> setDataSrc(String dataSrc) {
        this.dataSrc = dataSrc;

        return this;
    }
    
    public String getIdField() {
		return idField;
	}

	public DocumentLoadOnlyStoreFactory<K> setIdField(String idField) {
		this.idField = idField;
		return this;
	}

	public String getDataSrc() {
		return dataSrc;
	}

    /**
     * Get maximum batch size for write and delete operations.
     *
     * @return Maximum batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Set maximum batch size for write and delete operations.
     *
     * @param batchSize Maximum batch size.
     * @return {@code This} for chaining.
     */
    public DocumentLoadOnlyStoreFactory<K> setBatchSize(int batchSize) {
        this.batchSize = batchSize;

        return this;
    }    

    /**
     * Get maximum workers thread count. These threads are responsible for queries execution.
     *
     * @return Maximum workers thread count.
     */
    public int getMaximumPoolSize() {
        return maxPoolSize;
    }

    /**
     * Set Maximum workers thread count. These threads are responsible for queries execution.
     *
     * @param maxPoolSize Max workers thread count.
     * @return {@code This} for chaining.
     */
    public DocumentLoadOnlyStoreFactory<K> setMaximumPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;

        return this;
    }    

    /**
     * Parallel load cache minimum row count threshold.
     *
     * @return If {@code 0} then load sequentially.
     */
    public int getParallelLoadCacheMinimumThreshold() {
        return parallelLoadCacheMinThreshold;
    }

    /**
     * Parallel load cache minimum row count threshold.
     *
     * @param parallelLoadCacheMinThreshold Minimum row count threshold. If {@code 0} then load sequentially.
     * @return {@code This} for chaining.
     */
    public DocumentLoadOnlyStoreFactory<K> setParallelLoadCacheMinimumThreshold(int parallelLoadCacheMinThreshold) {
        this.parallelLoadCacheMinThreshold = parallelLoadCacheMinThreshold;

        return this;
    }  


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DocumentLoadOnlyStoreFactory.class, this);
    }

	public boolean isStreamerEnabled() {
		return streamerEnabled;
	}

	public DocumentLoadOnlyStoreFactory<K> setStreamerEnabled(boolean streamerEnabled) {
		this.streamerEnabled = streamerEnabled;
		return this;
	}

	public JdbcType[] getTypes() {
		return types;
	}

	public DocumentLoadOnlyStoreFactory<K> setTypes(JdbcType[] types) {
		this.types = types;
		return this;
	}
}
