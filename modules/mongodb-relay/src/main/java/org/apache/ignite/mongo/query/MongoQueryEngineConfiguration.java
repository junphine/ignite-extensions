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

package org.apache.ignite.mongo.query;


import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.QueryEngineConfigurationEx;

/**
 * Query engine configuration for Mongo sql query engine.
 */
public class MongoQueryEngineConfiguration implements QueryEngineConfigurationEx {
    /** Query engine name. */
    public static final String ENGINE_NAME = "mongo";
    /** */
    private boolean isDflt;


    /** {@inheritDoc} */
    @Override public String engineName() {
        return ENGINE_NAME;
    }

    /** {@inheritDoc} */
    @Override public Class<? extends QueryEngine> engineClass() {
        return MongoQueryEngine.class;
    }

    /** {@inheritDoc} */
    @Override public boolean isDefault() {
        return isDflt;
    }

    /** {@inheritDoc} */
    @Override public MongoQueryEngineConfiguration setDefault(boolean isDflt) {
        this.isDflt = isDflt;

        return this;
    }
}
