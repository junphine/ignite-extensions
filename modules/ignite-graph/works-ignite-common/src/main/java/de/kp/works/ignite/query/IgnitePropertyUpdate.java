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


public class IgnitePropertyUpdate extends IgniteQuery {
    protected HashMap<String, Object> fields = new HashMap<>();
    /**
     * Retrieves all elements that are referenced by
     * a certain label and share a certain property
     * key and value
     */
    public IgnitePropertyUpdate(String cacheName, IgniteAdmin admin, String label, String id, String key, Object value) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        fields.put(IgniteConstants.LABEL_COL_NAME, label);
        fields.put("id", id);
        
        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, key);
        fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, quoteSqlValue(value));

    }

    @Override
    protected void createSql() {
        try {
            sqlStatement = "update " + cache.getName();
            /*
             * Build the `clause` of the SQL statement
             * from the provided fields
             */
            if(this.elementType!=ElementType.DOCUMENT){ // "withProp"

                /*
                 * The value column must match the provided value
                 */
                String column = (String)fields.get(IgniteConstants.PROPERTY_VALUE_COL_NAME);
                if(column.equalsIgnoreCase(IgniteConstants.PROPERTY_WEIGHT_COL_NAME)){

                    sqlStatement += " set " + IgniteConstants.PROPERTY_WEIGHT_COL_NAME;
                    sqlStatement += " = " + fields.get(IgniteConstants.PROPERTY_VALUE_COL_NAME);

                    sqlStatement += " where " + this.idColumnName();
                    sqlStatement += " = '" + fields.get("id") + "'";

                    sqlStatement += " and " + IgniteConstants.LABEL_COL_NAME;
                    sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";
                }
                else {
                    sqlStatement += " set " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
                    sqlStatement += " = " + fields.get(IgniteConstants.PROPERTY_VALUE_COL_NAME);

                    sqlStatement += " where " + this.idColumnName();
                    sqlStatement += " = '" + fields.get("id") + "'";

                    sqlStatement += " and " + IgniteConstants.LABEL_COL_NAME;
                    sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";

                    sqlStatement += " and " + IgniteConstants.PROPERTY_KEY_COL_NAME;
                    sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "'";
                }

            }
            else {
            	sqlStatement += " set ";
	            sqlStatement += "  \"" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "\"";	
	            
	            sqlStatement += " = " + fields.get(IgniteConstants.PROPERTY_VALUE_COL_NAME);

                sqlStatement += " where " + this.idColumnName();
                sqlStatement += " = '" + fields.get("id") + "'";
	           
            }


        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
