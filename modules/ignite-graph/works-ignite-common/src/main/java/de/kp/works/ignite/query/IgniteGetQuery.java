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
import de.kp.works.ignite.graph.ElementType;

import java.util.List;

public class IgniteGetQuery extends IgniteQuery {

    private static final String ID_FIELD = "ID";

    private String id;

    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public IgniteGetQuery(String cacheName, IgniteAdmin admin, Object id) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        this.id = " = "+ quoteSqlValue(id);
    }

    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public IgniteGetQuery(String cacheName, IgniteAdmin admin, List<Object> ids) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        StringBuilder sqlStatement = new StringBuilder();

        if (ids.size() == 1) {
            sqlStatement.append(" = ").append(quoteSqlValue(ids.get(0)));
        }
        else {
            StringBuilder inPart = new StringBuilder();
            for (int i = 0; i < ids.size(); i++) {
                if (i > 0) inPart.append(",");
                inPart.append(quoteSqlValue(ids.get(i)));
            }
            sqlStatement.append(" IN (").append(inPart).append(")");
        }

        this.id = sqlStatement.toString();
    }

    @Override
    protected void createSql() {
        try {
            buildSelectPart();
            /*
             * Build the `where` clause and thereby distinguish
             * between a single or multiple identifier values
             */
            String keyField = this.idColumnName();

            Object ids = this.id;
            if (ids == null) {
                /*
                 * No identifier(s) provided: select nothing
                 */
                sqlStatement = null;
                return;
            }

            sqlStatement += " where " + keyField + ids;
            /*
             * Rows of a single element must be grouped
             */
            sqlStatement += " order by " + keyField;

        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
