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

import de.kp.works.ignite.IgniteColumn;
import de.kp.works.ignite.ValueType;

import java.util.ArrayList;
import java.util.List;

public class IgniteResult {
    final String label;
    Object id;
    short idType;
    long createdAt;
    long updatedAt;

    private List<IgniteColumn> columns = new ArrayList<>();

    /**
     * [IgniteResult] represents all cache entries
     * that refer to a certain edge, row, etc.
     */
    public IgniteResult(Object id, String label) {
        this.id = id;
        this.label = label;
    }

    public IgniteResult(String label) {
        this.label = label;
    }

    public void addColumn(String colName, String colType, Object colValue) {
    	if(!colName.isEmpty()) {
    		columns.add(new IgniteColumn(colName, colType, colValue));
    	}
    }

    public void addColumn(String colName, short colType, Object colValue) {
        if(!colName.isEmpty()) {
            columns.add(new IgniteColumn(colName, ValueType.valueOf(colType).name(), colValue));
        }
    }

    /**
     * This method returns the user identifier assigned
     * to edges, vertices and other
     */
    public Object getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public short getIdType() {
        return idType;
    }

    public void setIdType(short idType) {
        this.idType = idType;
    }


    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Object getValue(String key) {
        Object value = null;
        for (IgniteColumn column : columns) {
            if (column.getColName().equals(key)) {
                value = column.getColValue();
            }
        }
        return value;

    }

    public List<IgniteColumn> getColumns() {
        return columns;
    }

    public boolean isEmpty() {
         return columns.isEmpty();
    }
}
