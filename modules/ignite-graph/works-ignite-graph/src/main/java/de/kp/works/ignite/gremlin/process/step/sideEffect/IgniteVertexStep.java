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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public final class IgniteVertexStep<E extends Element> extends VertexStep<E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public IgniteVertexStep(final VertexStep<E> originalVertexStep) {
        super(originalVertexStep.getTraversal(), originalVertexStep.getReturnClass(),
              originalVertexStep.getDirection(), originalVertexStep.getEdgeLabels()
          );

        originalVertexStep.getLabels().forEach(this::addLabel);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        return Vertex.class.isAssignableFrom(getReturnClass()) ?
                (Iterator<E>) lookupVertices(traverser, this.hasContainers) :
                (Iterator<E>) lookupEdges(traverser, this.hasContainers);
    }

    private Iterator<Vertex> lookupVertices(final Traverser.Admin<Vertex> traverser, final List<HasContainer> hasContainers) {
        final Vertex start = traverser.get();
        /*
         * The incident edge list is served from the relation cache
         * (see EdgeModel), i.e. repeated traversals do not issue
         * another SQL query.
         */
        Iterator<Edge> edges = start.edges(getDirection(), getEdgeLabels());
        Iterator<Vertex> vertices = otherVertices(start, edges);

        if (hasContainers.isEmpty()) {
            /*
             * Without property predicates the neighbouring vertices
             * are returned lazily: properties are loaded (once) on
             * demand when they are actually accessed.
             */
            return vertices;
        }

        /*
         * The `has` predicates are evaluated against the element
         * properties; all neighbours are therefore loaded with a
         * single batch query.
         */
        List<Vertex> neighbours = new ArrayList<>();
        vertices.forEachRemaining(neighbours::add);

        if (!neighbours.isEmpty() && start.graph() instanceof IgniteGraph igniteGraph) {
            igniteGraph.getVertexModel().load(neighbours);
        }

        return CloseableIteratorUtils.filter(neighbours.iterator(),
                vertex -> HasContainer.testAll(vertex, hasContainers));
    }

    /**
     * Maps each incident edge to the vertex at the other end; the
     * resulting vertices are retrieved from the vertex cache (or
     * created, without loading their properties).
     */
    private Iterator<Vertex> otherVertices(final Vertex start, Iterator<Edge> edges) {
        final Object startId = start.id();
        final IgniteGraph graph = (IgniteGraph) start.graph();

        return IteratorUtils.map(edges, edge -> {
            Object inVertexId = edge.inVertex().id();
            Object outVertexId = edge.outVertex().id();

            Object vertexId = startId.equals(inVertexId) ? outVertexId : inVertexId;
            return graph.findOrCreateVertex(vertexId);
        });
    }

    private Iterator<Edge> lookupEdges(final Traverser.Admin<Vertex> traverser, final List<HasContainer> hasContainers) {
        return CloseableIteratorUtils.filter(traverser.get().edges(getDirection(), getEdgeLabels()),
                edge -> HasContainer.testAll(edge, hasContainers));
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return StringFactory.stepString(this, getDirection(), Arrays.asList(getEdgeLabels()), getReturnClass().getSimpleName().toLowerCase(), this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        /*
         * Flatten AND predicates (e.g. P.inside) so that each
         * single predicate can be evaluated independently.
         */
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
