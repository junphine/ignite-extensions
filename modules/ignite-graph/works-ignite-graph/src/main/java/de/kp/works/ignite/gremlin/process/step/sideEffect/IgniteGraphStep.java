package de.kp.works.ignite.gremlin.process.step.sideEffect;
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

import de.kp.works.ignite.gremlin.CloseableIteratorUtils;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.query.IgniteDocumentQuery;
import de.kp.works.ignite.query.IgniteEdgesQuery;
import de.kp.works.ignite.query.IgniteVertexQuery;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public final class IgniteGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public IgniteGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges());
    }

    private Iterator<E> edges() {
        if (null == this.ids)
            return Collections.emptyIterator();
        final IgniteGraph graph = (IgniteGraph) this.getTraversal().getGraph().get();
        return (Iterator)lookupEdges(graph, this.hasContainers, this.ids);
    }

    private Iterator<E> vertices() {
        if (null == this.ids)
            return Collections.emptyIterator();
        final IgniteGraph graph = (IgniteGraph) this.getTraversal().getGraph().get();
        return (Iterator)lookupVertices(graph, this.hasContainers, this.ids);
    }

    private Iterator<Vertex> lookupVertices(final IgniteGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        // ids are present, filter on them first
        if (ids.length > 0)
            return CloseableIteratorUtils.filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, hasContainers));
        ////// do index lookups //////
        // get a label being search on
        Optional<String> label = hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();

        int limit = 0;
        int offset = 0;
        if (label.isPresent()) {
            this.addLabel(label.get());
            IgniteVertexQuery igniteQuery = graph.getVertexModel().getTable().getVertexQuery(label.get());
            for(Object step: this.traversal.getSteps()){
                if(step instanceof RangeGlobalStep range){
                    offset = (int) range.getLowRange();
                    limit = (int)range.getHighRange();
                }
                else if (step instanceof PropertiesStep props){
                    props.getPropertyKeys();
                }
                else if (step instanceof HasStep kv){
                    List<HasContainer> hasValue = kv.getHasContainers();
                    for (HasContainer hasC : hasValue) {
                        if (hasC.getBiPredicate() instanceof Compare op) {
                            igniteQuery.addProperty(hasC.getKey(), hasC.getValue(),op);
                        }
                    }
                }
            }

            for (HasContainer hasC : hasContainers) {
                if (hasC.getBiPredicate() instanceof Compare op) {
                    igniteQuery.addProperty(hasC.getKey(), hasC.getValue(),op);
                }
            }
        	
        	if(limit>0) {
        		// find a vertex by label with limit
                igniteQuery.withRange(offset,limit);
        	}
            // find a vertex by label
            if(igniteQuery instanceof IgniteDocumentQuery documentQuery){
                return IteratorUtils.stream(graph.getDocumentModel(label.get()).verticesQuery(documentQuery))
                        .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
            }
            return IteratorUtils.stream(graph.getVertexModel().verticesQuery(igniteQuery))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        } else {
            // linear scan
            return CloseableIteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
        }
    }
    
    private Iterator<Edge> lookupEdges(final IgniteGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        // ids are present, filter on them first
        if (ids.length > 0)
            return CloseableIteratorUtils.filter(graph.edges(this.ids), edge -> HasContainer.testAll(edge, this.hasContainers));
        ////// do index lookups

        // get a label being search on
        List<String> labels = hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .map(hasContainer -> (String) hasContainer.getValue())
                .toList();

        int limit = 0;
        int offset = 0;
        if (!labels.isEmpty()) {
            labels.forEach(this::addLabel);
            IgniteEdgesQuery igniteQuery = graph.getEdgeModel().getTable().getEdgesQuery(labels);
            for(Object step: this.traversal.getSteps()) {
                if (step instanceof RangeGlobalStep range) {
                    offset = (int) range.getLowRange();
                    limit = (int) range.getHighRange();
                } else if (step instanceof PropertiesStep props) {
                    props.getPropertyKeys();
                } else if (step instanceof HasStep kv) {
                    List<HasContainer> hasValue = kv.getHasContainers();
                    for (HasContainer hasC : hasValue) {
                        if (hasC.getBiPredicate() instanceof Compare op) {
                            igniteQuery.addProperty(hasC.getKey(), hasC.getValue(),op);
                        }
                    }
                }
            }

            for (HasContainer hasC : hasContainers) {
                if (hasC.getBiPredicate() instanceof Compare op) {
                    igniteQuery.addProperty(hasC.getKey(), hasC.getValue(),op);
                }
            }
        	
        	if(limit>0) {
        		// find a vertex by label with limit
                igniteQuery.withRange(offset,limit);
        	}
            // find a edge by label
            return IteratorUtils.stream(graph.getEdgeModel().edgesQuery(igniteQuery))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        } else {
            // linear scan           
            return CloseableIteratorUtils.filter(graph.allEdges(), edge -> HasContainer.testAll(edge, this.hasContainers));
        }
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
