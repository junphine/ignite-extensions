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

import java.util.Map;

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
             * This query does not have a `where` clause
             * and just contains the `select` part
             */
        	if(sqlStatement==null || sqlStatement.isBlank()) {
        		sqlStatement = "select _key, * ";               
                sqlStatement += " from " + cache.getName();
                orderById();
        	}

        } catch (Exception e) {
            sqlStatement = null;
        }
    }
}
