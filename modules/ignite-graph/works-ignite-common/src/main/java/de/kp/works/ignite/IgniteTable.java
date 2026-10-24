package de.kp.works.ignite;
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

import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.mutate.*;
import de.kp.works.ignite.query.*;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IgniteTable extends IgniteBaseTable {

    public IgniteTable(String name, ElementType elementType, IgniteAdmin admin) {
        super(name, elementType, admin);
    }
    /**
     * This method adds or updates an Ignite cache entry;
     * note, the current implementation requires a fully
     * qualified cache entry.
     */
    public boolean put(IgnitePut ignitePut) throws Exception {

        if (admin == null) return false;
        if (!admin.igniteExists()) return false;

        try {

            if (elementType.equals(ElementType.EDGE)) {
                putEdge(ignitePut);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                putVertex(ignitePut);
            }
            else if (elementType.equals(ElementType.DOCUMENT)) {
            	putDocument(ignitePut);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");

            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }
    /**
     * Delete supports deletion of an entire element
     * (edge or vertex) and also the removal of a certain
     * property.
     */
    public boolean delete(IgniteDelete igniteDelete) throws Exception {

        if (admin == null) return false;
        if (!admin.igniteExists()) return false;

        try {

            if (elementType.equals(ElementType.EDGE)) {
                deleteEdge(igniteDelete);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                deleteVertex(igniteDelete);
            }
            else if (elementType.equals(ElementType.DOCUMENT)) {
                admin.deleteDocument(igniteDelete, name);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");

             return true;

        } catch (Exception e) {
            return false;
        }
    }

    public Object increment(IgniteIncrement igniteIncrement) throws Exception {

        if (admin == null) return false;
        if (!admin.igniteExists()) return false;
        /*
         * In case of an increment, the respective incremented
         * value is returned,
         */
        try {

            if (elementType.equals(ElementType.EDGE)) {
                return incrementEdge(igniteIncrement);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                return incrementVertex(igniteIncrement);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported increment.");


        } catch (Exception e) {
            return null;
        }

    }

    public void batch(List<IgniteMutation> mutations, Object[] results) throws Exception {

         for (int i = 0; i < mutations.size(); i++) {
             /*
              * Determine the respective mutation
              */
             IgniteMutation mutation = mutations.get(i);
             if (mutation.mutationType.equals(IgniteMutationType.DELETE)) {

                 // System.out.println("DELETE");
                 IgniteDelete deleteMutation = (IgniteDelete)mutation;

                 boolean success = delete(deleteMutation);
                 if (!success) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                         "Deletion of element '" + deleteMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = deleteMutation.getId();

             }
             else if (mutation.mutationType.equals(IgniteMutationType.INCREMENT)) {

                 IgniteIncrement incrementMutation = (IgniteIncrement)mutation;

                 Object value = increment(incrementMutation);
                 if (value == null) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                             "Increment of element '" + incrementMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = value;

             }
             else {

                 IgnitePut putMutation = (IgnitePut)mutation;

                 boolean success = put(putMutation);
                 if (!success) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                             "Put of element '" + putMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = putMutation.getId();

             }

        }
    }
    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public List<IgniteResult> get(List<Object> ids) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, admin, ids);

        Iterator<IgniteResult> it = igniteQuery.getResult();
        ArrayList<IgniteResult> result = new ArrayList<>();
		while(it.hasNext()) {
			result.add(it.next());
		}
        return result;
    }
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public IgniteResult get(Object id) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, admin, id);

        Iterator<IgniteResult> result = igniteQuery.getResult();
        if (!result.hasNext()) return null;
        return result.next();
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     */
    public IgniteQuery getAllQuery() {
        return new IgniteAllQuery(name, admin);
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label
     */
    public IgniteVertexQuery getVertexQuery(String label) {
        if (elementType.equals(ElementType.DOCUMENT)) {
            return new IgniteDocumentQuery(name, admin, label);
        }
        return new IgniteVertexQuery(name, admin, label);
    }

    /**
     * Returns an [IgniteVertexQuery] that retrieves all vertices which
     * refer to at least one of the provided labels; an empty label list
     * does not restrict the query by label.
     */
    public IgniteVertexQuery getVertexQuery(List<String> labels) {
        if (elementType.equals(ElementType.DOCUMENT) && labels != null && labels.size() == 1) {
            return new IgniteDocumentQuery(name, admin, labels.get(0));
        }
        return new IgniteVertexQuery(name, admin, labels);
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value
     */
    public IgniteQuery getPropertyQuery(String label, String key, Object value) {
        return new IgnitePropertyQuery(name, admin, label, key, value);
    }

    /* EDGE READ SUPPORT */

    /**
     * Method to find all edges that refer to the provided
     * vertex that match direction and the provided labels
     */
    public IgniteEdgesQuery getEdgesQuery(List<String> labels) {
        return new IgniteEdgesQuery(name, admin, labels);
    }

    public IgniteEdgesQuery getEdgesQuery(String... labels) {
        return new IgniteEdgesQuery(name, admin, List.of(labels));
    }

    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, and a property with
     * a specific value
     */
    public IgniteQuery getEdgesQuery(Object vertexId, Direction direction, String label,
                                     String key, Object value) {
        return new IgniteEdgesQuery(name, admin, List.of(label)).addProperty(vertexId, direction, key, value);
    }

    public IgniteDocumentQuery getIgniteSqlQuery(String sql) {
        /*
         * The SQL statement must be registered as such; passing it as
         * label (as before) let the query silently fall back to the
         * default `select` statement.
         */
        IgniteDocumentQuery igniteQuery = new IgniteDocumentQuery(name, admin, null);
        igniteQuery.setSqlStatment(sql);
    	return igniteQuery;
    }
}
