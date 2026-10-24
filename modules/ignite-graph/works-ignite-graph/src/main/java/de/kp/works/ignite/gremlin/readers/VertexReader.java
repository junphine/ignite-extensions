package de.kp.works.ignite.gremlin.readers;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import de.kp.works.ignite.ValueType;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.IgniteVertex;
import de.kp.works.ignite.gremlin.exception.IgniteGraphNotFoundException;
import de.kp.works.ignite.query.IgniteResult;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class VertexReader extends LoadingElementReader<Vertex> {

    public VertexReader(IgniteGraph graph) {
        super(graph);
    }

    @Override
    public Vertex parse(IgniteResult result) {
        Object id = result.getId();
        Vertex vertex = graph.findOrCreateVertex(id);
        load(vertex, result);
        return vertex;
    }

    @Override
    public void load(Vertex vertex, IgniteResult result) {
        if (result==null || result.isEmpty()) {
            throw new IgniteGraphNotFoundException(vertex, "Vertex does not exist: " + vertex.id());
        }
        String label   = result.getLabel();
        long createdAt = result.getCreatedAt();
        long updatedAt = result.getUpdatedAt();

        Map<String, Object> props = new HashMap<>();
        for (IgniteColumn column : result.getColumns()) {
            String colName = column.getColName();
            Object oriV = props.get(colName);
            if(oriV!=null) {
                if(oriV instanceof Collection) {
                    Collection<Object> list = (Collection)oriV;
                    list.add(column.getColValue());
                }
                else {
                    ValueType type = column.getColType();
                    if(type==ValueType.ARRAY || type==ValueType.LIST) {
                        List<Object> list = new ArrayList<>(4);
                        list.add(oriV);
                        list.add(column.getColValue());
                        props.put(colName, list);
                    }
                    else if(type==ValueType.SET) {
                        Set<Object> list = new HashSet<>(4);
                        list.add(oriV);
                        list.add(column.getColValue());
                        props.put(colName, list);
                    }
                    else{
                        // ignore dup column name
                    }
                }
            }
            else {
                props.put(colName, column.getColValue());
            }

        }

        IgniteVertex newVertex = new IgniteVertex(graph, vertex.id(), label, createdAt, updatedAt, props);
        ((IgniteVertex) vertex).copyFrom(newVertex);
    }
}
