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
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;

/**
 * Retrieves edges and supports the following (combinable) restrictions:
 *
 * <ul>
 *   <li>an incident vertex and a direction,</li>
 *   <li>one or more edge labels,</li>
 *   <li>property predicates registered via {@link IgniteQuery#addProperty},</li>
 *   <li>an element window registered via {@link IgniteQuery#withRange}.</li>
 * </ul>
 *
 * Note: an empty label list means "all labels" - not "no result".
 */
public class IgniteEdgesQuery extends IgniteQuery {

    private Direction direction;

    public IgniteEdgesQuery(String cacheName, IgniteAdmin admin, List<String> labels) {
        super(cacheName, admin);
        if (labels != null) this.labels.addAll(labels);
    }

    /**
     * Restricts the query to edges that are incident to the provided
     * vertex in the provided direction.
     */
    public IgniteEdgesQuery addVertex(Object vertex, Direction direction) {
        if (vertex != null && direction != null) {
            this.direction = direction;
            vertexToFields(vertex, direction, fields);
        }
        return this;
    }

    /**
     * Restricts the query to edges that are incident to the provided
     * vertex in the provided direction and share a certain property
     * key/value pair.
     *
     * The property predicate is converted into an element-id subquery,
     * i.e. all property rows of a matching edge are returned and the
     * resulting edges are complete.
     */
    public IgniteEdgesQuery addProperty(Object vertex, Direction direction, String key, Object value) {
        addVertex(vertex, direction);
        addProperty(key, value, Compare.eq);
        return this;
    }

    @Override
    protected void createSql() {
        try {
            addDirectionClause();
            addLabelClause();
            buildElementSql();

        } catch (Exception e) {
            sqlStatement = null;
        }

    }

    /**
     * Builds the incident-vertex restriction; the `BOTH` variant groups
     * the out and in conditions correctly:
     *
     * <pre>
     *   source = 'x' [and source_type = 't']
     *   to     = 'x' [and to_type     = 't']
     *   (source = 'x' [and source_type = 't']) or (to = 'x' [and to_type = 't'])
     * </pre>
     */
    private void addDirectionClause() {
        if (direction == null) return;

        if (direction == Direction.IN && fields.containsKey(IgniteConstants.TO_COL_NAME)) {
            addWhereClause(vertexCondition(IgniteConstants.TO_COL_NAME, IgniteConstants.TO_TYPE_COL_NAME));
        }
        else if (direction == Direction.OUT && fields.containsKey(IgniteConstants.FROM_COL_NAME)) {
            addWhereClause(vertexCondition(IgniteConstants.FROM_COL_NAME, IgniteConstants.FROM_TYPE_COL_NAME));
        }
        else if (direction == Direction.BOTH
                && fields.containsKey(IgniteConstants.FROM_COL_NAME)
                && fields.containsKey(IgniteConstants.TO_COL_NAME)) {
            StringBuilder both = new StringBuilder();
            both.append("(").append(vertexCondition(IgniteConstants.FROM_COL_NAME, IgniteConstants.FROM_TYPE_COL_NAME)).append(")");
            both.append(" or ");
            both.append("(").append(vertexCondition(IgniteConstants.TO_COL_NAME, IgniteConstants.TO_TYPE_COL_NAME)).append(")");
            addWhereClause("(" + both + ")");
        }
    }

    private String vertexCondition(String idColumn, String typeColumn) {
        StringBuilder condition = new StringBuilder();
        condition.append(idColumn).append(" = '").append(escapeValue(fields.get(idColumn))).append("'");
        if (fields.containsKey(typeColumn)) {
            condition.append(" and ").append(typeColumn).append(" = '")
                    .append(escapeValue(fields.get(typeColumn))).append("'");
        }
        return condition.toString();
    }
}
