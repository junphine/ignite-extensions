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
import de.kp.works.ignite.gremlin.models.DocumentModel;
import de.kp.works.ignite.query.IgniteDocumentQuery;
import de.kp.works.ignite.query.IgniteEdgesQuery;
import de.kp.works.ignite.query.IgniteQuery;
import de.kp.works.ignite.query.IgniteVertexQuery;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

public final class IgniteGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    /*
     * Element window: a RangeGlobalStep that directly follows this step is
     * removed by the IgniteGraphStepStrategy, and its window is applied
     * here - either as part of the SQL statement or in memory.
     * A negative value means: no window.
     */
    private long rangeLow = -1;
    private long rangeHigh = -1;

    @SuppressWarnings("unchecked")
    public IgniteGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges());
    }

    /**
     * Defines the element window that replaces a removed RangeGlobalStep.
     */
    public void setRange(final long low, final long high) {
        this.rangeLow = low;
        this.rangeHigh = high;
    }

    private boolean hasWindow() {
        return rangeLow >= 0 && rangeHigh > rangeLow;
    }

    private int windowOffset() {
        return (int) Math.min(rangeLow, Integer.MAX_VALUE);
    }

    private int windowCount() {
        return (int) Math.min(rangeHigh - rangeLow, Integer.MAX_VALUE);
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

    /* ======================== VERTEX LOOKUP ======================== */

    private Iterator<Vertex> lookupVertices(final IgniteGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        // ids are present, filter on them first
        if (ids.length > 0)
            return CloseableIteratorUtils.filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, hasContainers));

        final Set<String> requestedLabels = extractLabels(hasContainers);

        /*
         * Document labels are backed by separate per-label caches,
         * all remaining labels are stored in the shared vertex table.
         */
        final List<String> vertexLabels = new ArrayList<>();
        final List<String> documentLabels = new ArrayList<>();
        for (String label : requestedLabels) {
            if (isDocumentLabel(graph, label)) documentLabels.add(label);
            else vertexLabels.add(label);
        }

        /*
         * A single query on the vertex table serves all non-document
         * labels (and the unrestricted case).
         */
        if (documentLabels.isEmpty()) {
            if (requestedLabels.isEmpty()
                    && !hasWindow()
                    && !hasPushablePropertyPredicate(hasContainers)) {
                // linear scan
                return CloseableIteratorUtils.filter(graph.vertices(),
                        vertex -> HasContainer.testAll(vertex, hasContainers));
            }

            IgniteVertexQuery igniteQuery = graph.getVertexModel().getTable().getVertexQuery(vertexLabels);
            boolean allPushed = pushPredicates(igniteQuery, hasContainers, vertexLabels);

            boolean windowPushed = allPushed && hasWindow();
            if (windowPushed) igniteQuery.withRange(windowOffset(), windowCount());

            return finishVertices(graph.getVertexModel().verticesQuery(igniteQuery), windowPushed, hasContainers);
        }

        /*
         * At least one document label is requested: one query per
         * requested label (a document cache is label specific).
         */
        final List<QueryPart> parts = new ArrayList<>();
        if (!vertexLabels.isEmpty()) {
            IgniteVertexQuery igniteQuery = graph.getVertexModel().getTable().getVertexQuery(vertexLabels);
            boolean allPushed = pushPredicates(igniteQuery, hasContainers, vertexLabels);
            parts.add(new QueryPart(igniteQuery, allPushed,
                    () -> graph.getVertexModel().verticesQuery(igniteQuery)));
        }
        for (String label : documentLabels) {
            final DocumentModel model = graph.getDocumentModel(label);
            final IgniteVertexQuery igniteQuery = model.getTable().getVertexQuery(label);
            /*
             * Document caches do not support property pushdown, i.e. the
             * window can only be pushed down if the query is restricted
             * by labels only.
             */
            boolean allPushed = hasOnlyLabelPredicates(hasContainers);
            parts.add(new QueryPart(igniteQuery, allPushed,
                    () -> model.verticesQuery((IgniteDocumentQuery) igniteQuery)));
        }

        return runParts(parts, hasContainers);
    }

    /* ======================== EDGE LOOKUP ======================== */

    private Iterator<Edge> lookupEdges(final IgniteGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        // ids are present, filter on them first
        if (ids.length > 0)
            return CloseableIteratorUtils.filter(graph.edges(ids), edge -> HasContainer.testAll(edge, hasContainers));

        final List<String> labels = new ArrayList<>(extractLabels(hasContainers));

        if (labels.isEmpty()
                && !hasWindow()
                && !hasPushablePropertyPredicate(hasContainers)) {
            // linear scan
            return CloseableIteratorUtils.filter(graph.allEdges(),
                    edge -> HasContainer.testAll(edge, hasContainers));
        }

        IgniteEdgesQuery igniteQuery = graph.getEdgeModel().getTable().getEdgesQuery(labels);
        boolean allPushed = pushPredicates(igniteQuery, hasContainers, labels);

        boolean windowPushed = allPushed && hasWindow();
        if (windowPushed) igniteQuery.withRange(windowOffset(), windowCount());

        return finishEdges(graph.getEdgeModel().edgesQuery(igniteQuery), windowPushed, hasContainers);
    }

    /* ======================== RESULT ASSEMBLY ======================== */

    private Iterator<Vertex> finishVertices(final Iterator<Vertex> vertices, final boolean windowPushed,
                                           final List<HasContainer> hasContainers) {
        Iterator<Vertex> filtered = CloseableIteratorUtils.filter(vertices,
                vertex -> HasContainer.testAll(vertex, hasContainers));

        return (hasWindow() && !windowPushed) ? applyWindow(filtered) : filtered;
    }

    private Iterator<Edge> finishEdges(final Iterator<Edge> edges, final boolean windowPushed,
                                       final List<HasContainer> hasContainers) {
        Iterator<Edge> filtered = CloseableIteratorUtils.filter(edges,
                edge -> HasContainer.testAll(edge, hasContainers));

        return (hasWindow() && !windowPushed) ? applyWindow(filtered) : filtered;
    }

    /**
     * Executes the prepared query parts and applies all predicates that
     * were not pushed down in memory.
     *
     * The element window is only pushed down if a single query is
     * executed - otherwise it is applied to the union of the results.
     */
    private Iterator<Vertex> runParts(final List<QueryPart> parts, final List<HasContainer> hasContainers) {
        if (parts.size() == 1) {
            QueryPart part = parts.get(0);

            boolean windowPushed = part.allPushed && hasWindow();
            if (windowPushed) part.query.withRange(windowOffset(), windowCount());

            return finishVertices(part.supplier.get(), windowPushed, hasContainers);
        }

        final List<Iterator<Vertex>> iterators = new ArrayList<>(parts.size());
        for (QueryPart part : parts) {
            iterators.add(CloseableIteratorUtils.filter(part.supplier.get(),
                    vertex -> HasContainer.testAll(vertex, hasContainers)));
        }

        Iterator<Vertex> concatenated = new Iterator<Vertex>() {

            private int index = 0;

            @Override
            public boolean hasNext() {
                while (index < iterators.size()) {
                    if (iterators.get(index).hasNext()) return true;
                    index++;
                }
                return false;
            }

            @Override
            public Vertex next() {
                if (!hasNext()) throw new NoSuchElementException();
                return iterators.get(index).next();
            }
        };

        return hasWindow() ? applyWindow(concatenated) : concatenated;
    }

    /**
     * Applies the element window in memory; only used if the window
     * cannot be pushed down into SQL (or it refers to a union of
     * several query results).
     */
    private <T> Iterator<T> applyWindow(final Iterator<T> iterator) {
        long skip = rangeLow;
        while (skip > 0 && iterator.hasNext()) {
            iterator.next();
            skip--;
        }

        final long count = rangeHigh - rangeLow;
        return new Iterator<T>() {

            private long remaining = count;

            @Override
            public boolean hasNext() {
                return remaining > 0 && iterator.hasNext();
            }

            @Override
            public T next() {
                if (remaining <= 0) throw new NoSuchElementException();
                remaining--;
                return iterator.next();
            }
        };
    }

    /* ======================== HELPERS ======================== */

    /**
     * Extracts the labels a `hasLabel(...)` predicate refers to;
     * supported are `eq` and `within` with String values.
     */
    private static Set<String> extractLabels(final List<HasContainer> hasContainers) {
        final Set<String> labels = new LinkedHashSet<>();

        for (HasContainer hasContainer : hasContainers) {
            if (!T.label.getAccessor().equals(hasContainer.getKey())) continue;

            Object value = hasContainer.getValue();

            if (Compare.eq == hasContainer.getBiPredicate()) {
                if (value instanceof String) labels.add((String) value);
            }
            else if (Contains.within == hasContainer.getBiPredicate() && value instanceof Collection) {
                for (Object element : (Collection<?>) value) {
                    /*
                     * Element labels are always strings; non-string
                     * members can never match and are ignored.
                     */
                    if (element instanceof String) labels.add((String) element);
                }
            }
        }

        return labels;
    }

    private static boolean isDocumentLabel(final IgniteGraph graph, final String label) {
        if (label == null || label.isEmpty()) return false;
        return graph.isDocumentModel(label);
    }

    /**
     * Whether at least one property predicate can be pushed down.
     */
    private static boolean hasPushablePropertyPredicate(final List<HasContainer> hasContainers) {
        for (HasContainer hasContainer : hasContainers) {
            String key = hasContainer.getKey();
            if (T.label.getAccessor().equals(key)) continue;
            if (T.id.getAccessor().equals(key)) continue;

            if (hasContainer.getBiPredicate() instanceof Compare) {
                Compare op = (Compare) hasContainer.getBiPredicate();
                if (IgniteQuery.canPushPropertyFilter(op, hasContainer.getValue())) return true;
            }
        }
        return false;
    }

    /**
     * Pushes all pushable predicates into the provided query and
     * reports whether ALL predicates were pushed down.
     *
     * @param queryLabels labels the query is restricted to; a label
     *                    predicate is only considered as pushed if all
     *                    of its values refer to these labels
     */
    private static boolean pushPredicates(final IgniteQuery query, final List<HasContainer> hasContainers,
                                          final List<String> queryLabels) {
        boolean allPushed = true;

        for (HasContainer hasContainer : hasContainers) {
            String key = hasContainer.getKey();

            if (T.label.getAccessor().equals(key)) {
                if (!isLabelPushed(hasContainer, queryLabels)) allPushed = false;
                continue;
            }
            if (T.id.getAccessor().equals(key)) {
                allPushed = false;
                continue;
            }

            if (!(hasContainer.getBiPredicate() instanceof Compare)
                    || !query.isPropertyPushdownSupported()) {
                allPushed = false;
                continue;
            }

            Compare op = (Compare) hasContainer.getBiPredicate();
            Object value = hasContainer.getValue();

            if (!IgniteQuery.canPushPropertyFilter(op, value)) {
                allPushed = false;
                continue;
            }

            query.addProperty(key, value, op);
        }

        return allPushed;
    }

    private static boolean isLabelPushed(final HasContainer hasContainer, final List<String> queryLabels) {
        Object value = hasContainer.getValue();

        if (Compare.eq == hasContainer.getBiPredicate()) {
            return value instanceof String && queryLabels.contains(value);
        }
        if (Contains.within == hasContainer.getBiPredicate() && value instanceof Collection) {
            for (Object element : (Collection<?>) value) {
                if (element instanceof String && !queryLabels.contains(element)) return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Whether the predicates consist of label restrictions only; used
     * for document caches, which do not support property pushdown.
     */
    private static boolean hasOnlyLabelPredicates(final List<HasContainer> hasContainers) {
        for (HasContainer hasContainer : hasContainers) {
            if (!T.label.getAccessor().equals(hasContainer.getKey())) return false;
        }
        return true;
    }

    /**
     * A lazily executed query part (see {@link #runParts}).
     */
    private static final class QueryPart {
        final IgniteVertexQuery query;
        final boolean allPushed;
        final Supplier<Iterator<Vertex>> supplier;

        QueryPart(IgniteVertexQuery query, boolean allPushed, Supplier<Iterator<Vertex>> supplier) {
            this.query = query;
            this.allPushed = allPushed;
            this.supplier = supplier;
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
