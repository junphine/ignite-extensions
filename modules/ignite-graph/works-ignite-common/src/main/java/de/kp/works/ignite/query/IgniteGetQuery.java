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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IgniteGetQuery extends IgniteQuery {
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public IgniteGetQuery(String cacheName, IgniteAdmin admin, Object id) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, Object> fields = new HashMap<>();

        fields.put("ID"," = "+quoteSqlValue(id));
        createSql(fields);

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
        HashMap<String, Object> fields = new HashMap<>();
        
        StringBuilder sqlStatement = new StringBuilder("");       
        if (ids.size() == 1) {
            sqlStatement.append(" = " + quoteSqlValue(ids.get(0)));
        } else {
            List<String> inPart = Stream.of(ids)
                    .map(token -> quoteSqlValue(token)).collect(Collectors.toList());

            sqlStatement.append(" IN (" + String.join(",", inPart) + ")");
        }
        
        fields.put("ID", sqlStatement.toString());

        createSql(fields);


    }

    @Override
    protected void createSql(Map<String, Object> fields) {
        try {
            buildSelectPart();
            /*
             * Build the `where` clause and thereby distinguish
             * between a single or multiple identifier values
             */
            String keyField = IgniteConstants.VERTEX_ID_COL_NAME;
            if(this.elementType == ElementType.DOCUMENT) {
            	keyField = "_key";
            }
            else if(this.elementType == ElementType.EDGE) {
                keyField = IgniteConstants.EDGE_ID_COL_NAME;
            }
            sqlStatement += " where " + keyField;
            Object ids = fields.get("ID");
            sqlStatement += ids;

        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
