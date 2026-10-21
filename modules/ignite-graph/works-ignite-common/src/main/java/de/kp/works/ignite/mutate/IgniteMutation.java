package de.kp.works.ignite.mutate;
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
import de.kp.works.ignite.IgniteConstants;
import de.kp.works.ignite.graph.ElementType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IgniteMutation {

    public String label;

    public Object id;
    public List<IgniteColumn> columns = new ArrayList<>();

    public ElementType elementType;
    public IgniteMutationType mutationType;

    public IgniteMutation(Object id,String label) {
        this.id = id;
        this.label = label;
    }

    public List<IgniteColumn> getColumns() {
        return columns;
    }

    public List<String> getColumnNames() {
        return columns
                .stream().map(IgniteColumn::getColName)
                .collect(Collectors.toList());
    }

    public IgniteColumn getColumn(String columnName) {

        List<IgniteColumn> filtered = columns
                .stream()
                .filter((column) ->
                        column.getColName().equals(columnName)).collect(Collectors.toList());

        if (filtered.isEmpty()) return null;
        return filtered.get(0);

    }

    public Stream<IgniteColumn> getProperties() {
        return columns.stream()
                .filter(column -> {
                    switch (column.getColName()) {
                        case IgniteConstants.ID_TYPE_COL_NAME:
                        case IgniteConstants.VERTEX_ID_COL_NAME:
                        case IgniteConstants.EDGE_ID_COL_NAME:
                        case IgniteConstants.LABEL_COL_NAME:
                        case IgniteConstants.TO_COL_NAME:
                        case IgniteConstants.FROM_COL_NAME:
                        case IgniteConstants.CREATED_AT_COL_NAME:
                        case IgniteConstants.UPDATED_AT_COL_NAME:
                        case IgniteConstants.PROPERTY_WEIGHT_COL_NAME:
                            return false;

                        default:
                            return true;
                    }
                });
    }

    public Object getId() {
        return id;
    }
    public String getLabel() {
        return label;
    }

}
