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
import de.kp.works.ignite.IgniteConstants;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Retrieves all documents of a per-label document cache.
 *
 * Document caches contain a single row per document and derive their
 * table name from the label, i.e. there is neither a `label` column
 * nor a fixed property schema. Therefore only the element window
 * (see {@link IgniteQuery#withRange}) is pushed down - property
 * predicates are evaluated in memory.
 */
public class IgniteDocumentQuery extends IgniteVertexQuery {

    /**
     * Retrieve all elements that refer to the
     * selected Ignite cache
     */
    public IgniteDocumentQuery(String cacheName, IgniteAdmin admin, String label) {
        super(cacheName, admin, label);
    }

    public void setSqlStatment(String sql) {
    	this.sqlStatement = sql;
    }

    @Override
    protected void createSql() {
        try {
            /*
             * A custom SQL statement is used as is.
             */
            if (sqlStatement != null && !sqlStatement.isBlank()) return;

            /*
             * Document tables are label-specific and do not
             * provide a `label` column, i.e. no label clause
             * is added here.
             */
            buildElementSql();

        } catch (Exception e) {
            sqlStatement = null;
        }
    }

    @Override
    protected List<String> buildPropertyFilterClauses(String idColumn) {
        List<String> clauses = new ArrayList<>();
        if (propertyFilters.isEmpty() || !isPropertyPushdownSupported()) return clauses;

        // group predicates by property key, preserving registration order
        Map<String, List<PropertyFilter>> byKey = new LinkedHashMap<>();
        for (PropertyFilter filter : propertyFilters) {
            byKey.computeIfAbsent(filter.key, key -> new ArrayList<>()).add(filter);
        }
        for (Map.Entry<String, List<PropertyFilter>> entry : byKey.entrySet()) {
            StringBuilder condition = new StringBuilder();
            for (PropertyFilter filter : entry.getValue()) {
                if(!condition.isEmpty()){
                    condition.append(" and ");
                }
                condition.append(buildPropertyCondition(entry.getKey(),filter));
            }
            clauses.add(condition.toString());
        }
        return clauses;
    }

    private String buildPropertyCondition(String key,PropertyFilter filter) {
        String op = sqlOperator(filter.op);
        if (filter.value instanceof Number) {
            if (filter.op == Compare.eq || filter.op == Compare.neq) {
                // numeric equality uses the non indexed `property_value` column
                return key + filter.op + filter.value;
            }
            return key + op + filter.value;
        }
        return key + " " + op + " '" + escapeValue(filter.value) + "'";
    }
}
