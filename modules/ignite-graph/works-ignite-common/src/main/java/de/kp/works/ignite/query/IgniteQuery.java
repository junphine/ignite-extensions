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
import de.kp.works.ignite.IgniteResultTransform;
import de.kp.works.ignite.ValueUtils;
import de.kp.works.ignite.graph.EdgeEntryIterator;
import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.graph.IgniteEdgeEntry;
import de.kp.works.ignite.graph.IgniteVertexEntry;
import de.kp.works.ignite.graph.VertexEntryIterator;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class IgniteQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteQuery.class);

    protected IgniteCache<String, BinaryObject> cache;
    protected String sqlStatement;

    protected ElementType elementType;

    /*
     * Transform the provided properties into fields 字段名
     */
    protected HashMap<String, Object> fields = new HashMap<>();

    /**
     * Labels this query is restricted to; an empty list
     * means that no label restriction is applied.
     */
    protected final List<String> labels = new ArrayList<>();

    /**
     * Structured property predicates (see {@link #addProperty(String, Object, Compare)})
     * that are pushed down into the SQL statement.
     */
    protected final List<PropertyFilter> propertyFilters = new ArrayList<>();

    /**
     * `where` clauses (without the `where` keyword) that are combined
     * with `and` when the SQL statement is assembled.
     */
    protected final List<String> whereClauses = new ArrayList<>();

    /*
     * Optional window (see withRange); `limit < 0` means: no window.
     * NOTE: withRange must be called BEFORE the query is executed.
     */
    protected int offset = 0;
    protected int limit = -1;

    /**
     * A single property predicate; predicates are grouped by key when
     * the SQL statement is built, i.e. all predicates that refer to the
     * same property key end up in the same row-level subquery.
     */
    protected static class PropertyFilter {
        final String key;
        final Compare op;
        final Object value;

        PropertyFilter(String key, Compare op, Object value) {
            this.key = key;
            this.op = op;
            this.value = value;
        }
    }


    public IgniteQuery(String name, IgniteAdmin admin) {

        try {
            /*
             * Retrieve element type from provided
             * cache (table) name
             */
            if (name == null) {
                elementType = ElementType.UNDEFINED;
            }
            else if (name.equals(admin.namespace() + "_" + IgniteConstants.EDGES)) {
                elementType = ElementType.EDGE;
            }
            else if (name.equals(admin.namespace() + "_" + IgniteConstants.VERTICES)) {
                elementType = ElementType.VERTEX;
            }
            else if (name.startsWith(admin.namespace() + "_")) {
                elementType = ElementType.DOCUMENT;
            }
            else if (name.startsWith(admin.namespace() + ".")) {
                elementType = ElementType.DOCUMENT;
            }
            else
                elementType = ElementType.UNDEFINED;
            /*
             * Connect to Ignite cache of the respective
             * name
             */
            cache = admin.createTable(name);

        } catch (Exception e) {
            cache = null;
        }

    }

    /**
     * Quotes a value for inline usage within a SQL statement;
     * embedded single quotes are escaped to support arbitrary
     * string values (and to avoid SQL injection).
     */
    protected String quoteSqlValue(Object v){
        if(v == null) {
            return "null";
        }
        if(v instanceof Number){
            return v.toString();
        }
        return "'" + v.toString().replace("'", "''") + "'";
    }

    /**
     * Escapes a string for usage within single quotes.
     */
    protected static String escapeValue(Object v) {
        if (v == null) return "null";
        return v.toString().replace("'", "''");
    }

    protected void vertexToFields(Object vertex, Direction direction, HashMap<String, Object> fields) {
        /*
         * An Edge links two Vertex objects. The Direction determines
         * which Vertex is the tail Vertex (out Vertex) and which Vertex
         * is the head Vertex (in Vertex).
         *
         * [HEAD VERTEX | OUT] -- <EDGE> --> [TAIL VERTEX | IN]
         *
         * The illustration is taken from the Apache TinkerPop [Edge]
         * documentation.
         *
         * This implies: FROM = OUT & TO = IN
         */
        
        
    	if (vertex instanceof ReferenceVertexProperty) {
			ReferenceVertexProperty<?> key = (ReferenceVertexProperty) vertex;        	
        	if (direction.equals(Direction.BOTH)) {
                fields.put(IgniteConstants.TO_COL_NAME, key.id());
                fields.put(IgniteConstants.FROM_COL_NAME, key.id());
                fields.put(IgniteConstants.TO_TYPE_COL_NAME, key.label());
                fields.put(IgniteConstants.FROM_TYPE_COL_NAME, key.label());
            }
            if (direction.equals(Direction.IN)) {
                fields.put(IgniteConstants.TO_COL_NAME, key.id());
                fields.put(IgniteConstants.TO_TYPE_COL_NAME, key.label());
            }
            else {
                fields.put(IgniteConstants.FROM_COL_NAME, key.id());
                fields.put(IgniteConstants.FROM_TYPE_COL_NAME, key.label());
            }
        }
        else {
        	if (direction.equals(Direction.BOTH)) {
                fields.put(IgniteConstants.TO_COL_NAME, vertex.toString());
                fields.put(IgniteConstants.FROM_COL_NAME, vertex.toString());
            }
            if (direction.equals(Direction.IN))
                fields.put(IgniteConstants.TO_COL_NAME, vertex.toString());
            else
                fields.put(IgniteConstants.FROM_COL_NAME, vertex.toString());
        }

    }


    /**
     * Determines whether a `has(...)` predicate can safely be pushed
     * down into SQL. Only predicates whose SQL representation is a
     * SUPERSET of the in-memory result are pushed down; all remaining
     * predicates are still evaluated in memory by the caller.
     *
     * Supported: `eq` on any non-null value, and the ordering predicates
     * `gt`, `gte`, `lt` and `lte` on numeric (finite) values.
     */
    public static boolean canPushPropertyFilter(Compare op, Object value) {
        if (value == null) return false;
        if (op == Compare.eq) {
            /*
             * Only values whose string representation is what has been
             * stored in `property_value` (String, Boolean) or that can be
             * compared via `property_number` (Number) are supported;
             * all other types (e.g. Date, UUID) remain in memory.
             */
            return value instanceof String || value instanceof Number || value instanceof Boolean;
        }
        if (op == Compare.gt || op == Compare.gte || op == Compare.lt || op == Compare.lte) {
            if (!(value instanceof Number)) return false;
            double d = ((Number) value).doubleValue();
            return !Double.isNaN(d) && !Double.isInfinite(d);
        }
        /*
         * neq, within, without and all other predicates are not pushed down:
         * row-level semantics differ from element-level semantics in corner
         * cases (e.g. cross-type numeric equality), so they stay in memory.
         */
        return false;
    }

    /**
     * Registers a property predicate for SQL pushdown. Predicates that
     * cannot be pushed down (see {@link #canPushPropertyFilter}) as well
     * as predicates on the reserved keys `id` and `label` are ignored;
     * callers must apply them in memory.
     *
     * @return this query (fluent API)
     */
    public IgniteQuery addProperty(String key, Object value, Compare op) {
        if (key == null) return this;
        if (T.id.getAccessor().equals(key) || T.label.getAccessor().equals(key)) return this;
        if (!canPushPropertyFilter(op, value)) return this;

        propertyFilters.add(new PropertyFilter(key, op, value));
        return this;
    }

    /**
     * Whether property predicates registered via {@link #addProperty}
     * are translated into SQL. Document caches store each document as a
     * single row with arbitrary columns, so property pushdown is disabled
     * there (predicates are evaluated in memory).
     */
    public boolean isPropertyPushdownSupported() {
        return elementType != ElementType.DOCUMENT;
    }

    /**
     * Defines an element window (offset &gt;= 0, limit &gt; 0) that is
     * pushed down as a DISTINCT element-id window, i.e. `limit` counts
     * elements and not storage rows.
     *
     * NOTE: this method must be called BEFORE the query is executed.
     *
     * @return this query (fluent API)
     */
    public IgniteQuery withRange(int offset, int limit) {
        this.offset = Math.max(offset, 0);
        this.limit = limit;
        return this;
    }

    protected boolean hasRange() {
        return limit > 0;
    }

    protected void addWhereClause(String clause) {
        if (clause != null && !clause.isEmpty()) whereClauses.add(clause);
    }

    /**
     * Translates the configured labels into a `where` clause
     * (`label = 'x'` or `label IN ('a','b')`); an empty label
     * list does not restrict the query.
     */
    protected void addLabelClause() {
        if (labels.isEmpty()) return;

        StringBuilder builder = new StringBuilder();
        builder.append(IgniteConstants.LABEL_COL_NAME);
        if (labels.size() == 1) {
            builder.append(" = '").append(escapeValue(labels.get(0))).append("'");
        }
        else {
            builder.append(" IN (");
            for (int i = 0; i < labels.size(); i++) {
                if (i > 0) builder.append(",");
                builder.append("'").append(escapeValue(labels.get(i))).append("'");
            }
            builder.append(")");
        }
        addWhereClause(builder.toString());
    }

    /**
     * The name of the column that identifies an element (data row group).
     */
    protected String idColumnName() {
        if (elementType == ElementType.EDGE) return IgniteConstants.EDGE_ID_COL_NAME;
        if (elementType == ElementType.DOCUMENT) return "_key";
        return IgniteConstants.VERTEX_ID_COL_NAME;
    }

    /**
     * Property predicates refer to a SINGLE property row, while elements
     * span one row per property. Therefore each distinct property key is
     * translated into an element-id subquery:
     *
     * <pre>
     *   &lt;idColumn&gt; IN (SELECT DISTINCT &lt;idColumn&gt; FROM &lt;cache&gt;
     *                   WHERE label = 'x' AND property_key = 'k' AND ... )
     * </pre>
     *
     * This keeps the outer query free of row-level restrictions, i.e. all
     * rows of a matching element are returned and the resulting elements
     * are complete (all properties present).
     */
    protected List<String> buildPropertyFilterClauses(String idColumn) {
        List<String> clauses = new ArrayList<>();
        if (propertyFilters.isEmpty() || !isPropertyPushdownSupported()) return clauses;

        // group predicates by property key, preserving registration order
        Map<String, List<PropertyFilter>> byKey = new LinkedHashMap<>();
        for (PropertyFilter filter : propertyFilters) {
            byKey.computeIfAbsent(filter.key, key -> new ArrayList<>()).add(filter);
        }

        /*
         * All rows of an element share the same label, therefore the label
         * restriction can safely be added to the subquery - which makes the
         * composite (label, property_key) index usable.
         */
        String labelPart = (labels.isEmpty() || elementType == ElementType.DOCUMENT)
                ? "" : " AND " + labelClauseText();

        for (Map.Entry<String, List<PropertyFilter>> entry : byKey.entrySet()) {
            StringBuilder condition = new StringBuilder();
            condition.append(IgniteConstants.PROPERTY_KEY_COL_NAME);
            condition.append(" = '").append(escapeValue(entry.getKey())).append("'");
            for (PropertyFilter filter : entry.getValue()) {
                condition.append(" AND ").append(buildPropertyCondition(filter));
            }
            clauses.add(idColumn + " in (select distinct " + idColumn
                    + " from " + cache.getName() + labelPart
                    + " and " + condition + ")");
        }
        return clauses;
    }

    private String labelClauseText() {
        if (labels.size() == 1)
            return IgniteConstants.LABEL_COL_NAME + " = '" + escapeValue(labels.get(0)) + "'";

        StringBuilder builder = new StringBuilder(IgniteConstants.LABEL_COL_NAME).append(" IN (");
        for (int i = 0; i < labels.size(); i++) {
            if (i > 0) builder.append(",");
            builder.append("'").append(escapeValue(labels.get(i))).append("'");
        }
        return builder.append(")").toString();
    }

    private String buildPropertyCondition(PropertyFilter filter) {
        String op = sqlOperator(filter.op);
        if (filter.op == Compare.eq && filter.value instanceof Number) {
            // numeric equality uses the indexed `property_number` column
            return IgniteConstants.PROPERTY_NUMBER_COL_NAME + " = " + filter.value;
        }
        if (filter.value instanceof Number) {
            return IgniteConstants.PROPERTY_NUMBER_COL_NAME + " " + op + " " + filter.value;
        }
        return IgniteConstants.PROPERTY_VALUE_COL_NAME + " " + op + " '" + escapeValue(filter.value) + "'";
    }

    private static String sqlOperator(Compare op) {
        if (op == Compare.gt) return ">";
        if (op == Compare.gte) return ">=";
        if (op == Compare.lt) return "<";
        if (op == Compare.lte) return "<=";
        return "=";
    }

    /**
     * Assembles a complete element query:
     *
     * <pre>
     *   select &lt;columns&gt; from &lt;cache&gt;
     *   [ where &lt;clauses&gt; [and &lt;idColumn&gt; in (&lt;element window subquery&gt;)] ]
     *   order by &lt;idColumn&gt;
     * </pre>
     *
     * The optional element window (see {@link #withRange}) is applied to
     * DISTINCT element ids - not to storage rows - so that it matches the
     * semantics of Gremlin's range/limit/skip steps and never truncates
     * an element in the middle of its property rows.
     */
    protected void buildElementSql() throws Exception {
        buildSelectPart();

        String idColumn = idColumnName();
        List<String> clauses = new ArrayList<>(whereClauses);
        if (isPropertyPushdownSupported()) {
            clauses.addAll(buildPropertyFilterClauses(idColumn));
        }

        boolean hasWhere = !clauses.isEmpty();
        if (hasWhere) {
            sqlStatement += " where " + String.join(" and ", clauses);
        }

        if (hasRange()) {
            StringBuilder window = new StringBuilder();
            window.append("select distinct ").append(idColumn).append(" from ").append(cache.getName());
            if (hasWhere) window.append(" where ").append(String.join(" and ", clauses));
            window.append(" order by ").append(idColumn);
            if (offset > 0) window.append(" limit ").append(offset).append(",").append(limit);
            else window.append(" limit ").append(limit);

            sqlStatement += hasWhere ? " and " : " where ";
            sqlStatement += idColumn + " in (" + window + ")";
        }

        sqlStatement += " order by " + idColumn;
    }

    protected IgniteQuery orderById() {
        if(elementType == ElementType.VERTEX) {
            orderBy(IgniteConstants.VERTEX_ID_COL_NAME,false);
        }
        else if(elementType == ElementType.EDGE){
            orderBy(IgniteConstants.EDGE_ID_COL_NAME,false);
        }
        return this;
    }

    
    public IgniteQuery orderBy(String order, boolean desc) {
        if(desc) {
        	sqlStatement += " order by  " + order + " DESC";
        }
        else {
        	sqlStatement += " order by  " + order;
        }
        return this;
    }

    public Iterator<IgniteEdgeEntry> getEdgeEntries() {

        List<IgniteEdgeEntry> entries = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            this.createSql();

        if (elementType == null || !elementType.equals(ElementType.EDGE))
            return entries.iterator();

        try {
        	FieldsQueryCursor<List<?>> sqlResult = getSqlResult();
            /*
             * Parse sql result and extract edge specific entries
             */
            return parseEdges(sqlResult);

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);

        }

        return entries.iterator();
    }

    public Iterator<IgniteVertexEntry> getVertexEntries() {

        List<IgniteVertexEntry> entries = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            this.createSql();

        if (elementType == null || !elementType.equals(ElementType.VERTEX))
            return entries.iterator();

        try {
        	FieldsQueryCursor<List<?>> sqlResult = getSqlResult();
            /*
             * Parse sql result and extract Vertex specific entries
             */
            return parseVertices(sqlResult);

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);

        }

        return entries.iterator();
    }


    /**
     * This method returns the result of the Ignite SQL query
     * in form of a row-based result list. It supports user
     * specific read requests
     */
    public Iterator<IgniteResult> getResult() {

        List<IgniteResult> result = new ArrayList<>();

        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            this.createSql();

        try {
            if (cache == null)
                throw new Exception("Cache is not initialized.");

            
            String cacheName = cache.getName();

            if (elementType.equals(ElementType.EDGE)) {
            	FieldsQueryCursor<List<?>> sqlResult = getSqlResult();
                /*
                 * Parse sql result and extract edge specific entries
                 */
            	EdgeEntryIterator entries = parseEdges(sqlResult);
                /*
                 * Group edge entries into edge rows
                 */
                return IgniteResultTransform.transformEdgeEntries(entries);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
            	FieldsQueryCursor<List<?>> sqlResult = getSqlResult();
                /*
                 * Parse sql result and extract Vertex specific entries
                 */
            	VertexEntryIterator entries = parseVertices(sqlResult);
                /*
                 * Group vertex entries into edge rows
                 */
                return IgniteResultTransform.transformVertexEntries(entries);

            }
            else if (elementType.equals(ElementType.DOCUMENT)) {
            	 /*
                 * Parse sql result and extract Document specific entries
                 */
                List<IgniteResult> entries = getSqlResultWithMeta(null);
                return entries.iterator();
            }
            else
                throw new Exception("Cache '" + cacheName +  "' is not supported.");

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);
        }
        return result.iterator();

    }
    
    private EdgeEntryIterator parseEdges(FieldsQueryCursor<List<?>> sqlResult) {
        return new EdgeEntryIterator(sqlResult);
    }

    private VertexEntryIterator parseVertices(FieldsQueryCursor<List<?>> sqlResult) {        
        return new VertexEntryIterator(sqlResult);
    }

    protected abstract void createSql();

    protected List<List<?>> getSqlResultList() {
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            this.createSql();
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        return cache.query(sqlQuery).getAll();
    }
    
    protected FieldsQueryCursor<List<?>> getSqlResult() {
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            this.createSql();
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        return cache.query(sqlQuery);
    }

    /** query from document */
    public List<IgniteResult> getSqlResultWithMeta(Object[] args) {
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            this.createSql();

        List<IgniteResult> result = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * could not be created (e.g. cache not initialized).
         */
        if (sqlStatement == null) return result;

        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        if(args!=null){
            sqlQuery.setArgs(args);
        }
        FieldsQueryCursor<List<?>> cursor = cache.query(sqlQuery);
        List<List<?>> sqlResult = cursor.getAll();
    	for(List<?> row: sqlResult) {
    		IgniteResult item = new IgniteResult(cache.getName());
    		for(int i=0;i<cursor.getColumnsCount();i++) {
    			Object value = row.get(i);
    			if(value!=null) {
    				String field = cursor.getFieldName(i);
    				if(field.equals("_key")) {
    					item.id = value;
                        continue;
    				}
                    if(field.equalsIgnoreCase("created_at") || field.equalsIgnoreCase("created")) {
                        Long up = ValueUtils.convertToLongValue(value);
                        if(up!=null){
                            item.createdAt = up;
                        }
                        continue;
                    }
                    if(field.equalsIgnoreCase("updated_at") || field.equalsIgnoreCase("updated")) {
                        Long up = ValueUtils.convertToLongValue(value);
                        if(up!=null){
                            item.updatedAt = up;
                        }
                        continue;
                    }
    				item.addColumn(field, value.getClass().getSimpleName(), value);
    			}
            }
    		result.add(item);
    	}
    	return result;
    }

    protected void buildSelectPart() throws Exception {

        if (cache == null)
            throw new Exception("Cache is not initialized.");

        List<String> columns = getColumns();

        sqlStatement = "select";
        sqlStatement += " " + String.join(",", columns);
        sqlStatement += " from " + cache.getName();

    }
    /**
     * This method retrieves the `select` columns
     * of the respective cache.
     */
    protected List<String> getColumns() throws Exception {

        List<String> columns = new ArrayList<>();
        /*
         * We want to retrieve the cache key as well
         */
        columns.add("_key");

        if (cache == null)
            throw new Exception("Cache is not initialized.");

        String cacheName = cache.getName();
        if (elementType.equals(ElementType.EDGE)) {
            /*
             * The edge identifier used by TinkerPop to
             * identify an equivalent of a data row
             */
            columns.add(IgniteConstants.EDGE_ID_COL_NAME);
            /*
             edge weight for prop name
             */
            columns.add(IgniteConstants.PROPERTY_WEIGHT_COL_NAME);
            /*
             * The edge label used by TinkerPop and IgniteGraph
             */
            columns.add(IgniteConstants.LABEL_COL_NAME);
            /*
             * The `TO` vertex description
             */
            columns.add(IgniteConstants.TO_COL_NAME);
            columns.add(IgniteConstants.TO_TYPE_COL_NAME);
            /*
             * The `FROM` vertex description
             */
            columns.add(IgniteConstants.FROM_COL_NAME);
            columns.add(IgniteConstants.FROM_TYPE_COL_NAME);
            /*
             * The timestamp this cache entry has been created.
             */
            columns.add(IgniteConstants.CREATED_AT_COL_NAME);
            /*
             * The timestamp this cache entry has been updated.
             */
            columns.add(IgniteConstants.UPDATED_AT_COL_NAME);
            /*
             * The property section of this cache entry
             */
            columns.add(IgniteConstants.PROPERTY_KEY_COL_NAME);
            columns.add(IgniteConstants.PROPERTY_TYPE_COL_NAME);
            /*
             * The serialized property value
             */
            columns.add(IgniteConstants.PROPERTY_VALUE_COL_NAME);
            return columns;
        }
        if (elementType.equals(ElementType.VERTEX)) {
            /*
             * The vertex identifier used by TinkerPop to identify
             * an equivalent of a data row
             */
            columns.add(IgniteConstants.VERTEX_ID_COL_NAME);
            /*
             * The vertex identifier type to reconstruct the
             * respective value. IgniteGraph supports [Long]
             * as well as [String] as identifier.
             */
            columns.add(IgniteConstants.ID_TYPE_COL_NAME);
            /*
             * The vertex label used by TinkerPop and IgniteGraph
             */
            columns.add(IgniteConstants.LABEL_COL_NAME);
            /*
             * The timestamp this cache entry has been created.
             */
            columns.add(IgniteConstants.CREATED_AT_COL_NAME);
            /*
             * The timestamp this cache entry has been updated.
             */
            columns.add(IgniteConstants.UPDATED_AT_COL_NAME);
            /*
             * The property section of this cache entry
             */
            columns.add(IgniteConstants.PROPERTY_KEY_COL_NAME);
            columns.add(IgniteConstants.PROPERTY_TYPE_COL_NAME);
            /*
             * The serialized property value
             */
            columns.add(IgniteConstants.PROPERTY_VALUE_COL_NAME);
            return columns;
        }
        if (elementType.equals(ElementType.DOCUMENT)) {
        	columns.add("*");
        	return columns;
        }
        throw new Exception("Cache '" + cacheName +  "' is not supported.");

    }
}
