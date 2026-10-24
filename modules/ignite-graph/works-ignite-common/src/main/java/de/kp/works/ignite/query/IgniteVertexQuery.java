package de.kp.works.ignite.query;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.IgniteAdmin;
import de.kp.works.ignite.graph.ElementType;

import java.util.List;

/**
 * Retrieves all vertices that match the provided label(s), the
 * property predicates registered via {@link IgniteQuery#addProperty}
 * and the optional element window (see {@link IgniteQuery#withRange}).
 */
public class IgniteVertexQuery extends IgniteQuery {

    /**
     * Retrieve all vertices that refer to the provided label.
     */
    public IgniteVertexQuery(String cacheName, IgniteAdmin admin, String label) {
        this(cacheName, admin, label == null ? List.of() : List.of(label));
    }

    /**
     * Retrieve all vertices that refer to at least one of the provided
     * labels; an empty list does not restrict the query by label.
     */
    public IgniteVertexQuery(String cacheName, IgniteAdmin admin, List<String> labels) {
        super(cacheName, admin);
        if (labels != null) this.labels.addAll(labels);
    }

    @Override
    protected void createSql() {
        try {
            /*
             * `label` restriction and all registered property predicates
             * are pushed down; predicates that are not pushable are still
             * applied by the caller (in-memory post filter).
             *
             * Document caches are label specific and provide no `label`
             * column, i.e. no label clause is added for them.
             */
            if (elementType != ElementType.DOCUMENT) addLabelClause();
            /*
             * Assemble select/where/order-by (and the optional element
             * window) parts of the SQL statement.
             */
            buildElementSql();

        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
