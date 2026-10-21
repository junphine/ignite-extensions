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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class IgniteSqlQuery extends IgniteQuery {

    /**
     * Retrieve all elements that refer to the
     * selected Ignite cache
     */
    public IgniteSqlQuery(String cacheName, IgniteAdmin admin, String sql) {
        super(cacheName, admin);
        this.sqlStatement = sql;

        /*
         * This query does not take fields
         */

        createSql(null);
    }
    
    public void setSqlStatment(String sql) {
    	this.sqlStatement = sql;
    }

    @Override
    protected void createSql(Map<String,Object> fields) {
        try {
            /*
             * This query does not have a `where` clause
             * and just contains the `select` part
             */
        	if(sqlStatement==null || sqlStatement.isBlank()) {
        		sqlStatement = "select _key, * ";               
                sqlStatement += " from " + cache.getName();
        	}

        } catch (Exception e) {
            sqlStatement = null;
        }
    }
}
