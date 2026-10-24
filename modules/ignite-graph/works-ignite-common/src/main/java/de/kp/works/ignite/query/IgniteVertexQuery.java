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
import de.kp.works.ignite.ValueUtils;
import de.kp.works.ignite.graph.ElementType;

import java.util.HashMap;
import java.util.Map;

public class IgniteVertexQuery extends IgniteQuery {

    /**
     * Retrieve all elements that refer to the provided
     * label, either edges or vertices.
     */
    public IgniteVertexQuery(String cacheName, IgniteAdmin admin, String label) {
        super(cacheName, admin);
        fields.put(IgniteConstants.LABEL_COL_NAME, label);
    }

    @Override
    protected void createSql() {
        try {
            buildSelectPart();
            /*
             * Build the `clause` of the SQL statement
             * from the provided fields
             */
            if(this.elementType != ElementType.DOCUMENT) {
            	
            	sqlStatement += " where " + IgniteConstants.LABEL_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";

                createWhereSql();
            }

            orderById();

        } catch (Exception e) {
            sqlStatement = null;
        }

    }

    protected void createWhereSql() {
        try {

            sqlStatement += " and " + IgniteConstants.PROPERTY_KEY_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "'";
            /*
             * The value of the value column must in the range of
             * INCLUSIVE_FROM_VALUE >= PROPERTY_VALUE_COL_NAME < EXCLUSIVE_TO_VALUE
             */
            boolean hasNum = ValueUtils.hasNumberValue(fields.get(IgniteConstants.INCLUSIVE_FROM_VALUE));
            if(hasNum) {
                sqlStatement += " and " + IgniteConstants.PROPERTY_NUMBER_COL_NAME;
                sqlStatement += " >= " + fields.get(IgniteConstants.INCLUSIVE_FROM_VALUE) + "";

                sqlStatement += " and " + IgniteConstants.PROPERTY_NUMBER_COL_NAME;
                sqlStatement += " < " + fields.get(IgniteConstants.EXCLUSIVE_TO_VALUE) + "";
            }
            else{
                sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
                sqlStatement += " >= '" + fields.get(IgniteConstants.INCLUSIVE_FROM_VALUE) + "'";

                sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
                sqlStatement += " < '" + fields.get(IgniteConstants.EXCLUSIVE_TO_VALUE) + "'";
            }


        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
