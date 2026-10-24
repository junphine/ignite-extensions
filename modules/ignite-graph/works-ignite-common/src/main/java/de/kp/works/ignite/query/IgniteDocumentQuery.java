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
}
