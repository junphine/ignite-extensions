package de.kp.works.ignite.gremlin.models;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import de.kp.works.ignite.gremlin.readers.VertexReader;
import de.kp.works.ignite.query.IgniteDocumentQuery;
import de.kp.works.ignite.query.IgniteVertexQuery;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import de.kp.works.ignite.IgniteResultTransform;
import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignite.gremlin.IgniteGraph;

import de.kp.works.ignite.gremlin.readers.DocumentReader;
import de.kp.works.ignite.query.IgniteResult;

public class DocumentModel extends VertexModel {
	final String label;

	public DocumentModel(IgniteGraph graph, IgniteTable table, String label) {		
		super(graph, table);
		this.label = label;
	}
	
	@Override
	public DocumentReader getReader() {		
        return new DocumentReader(graph, label);
    }


	public Iterator<Vertex> verticesQuery(IgniteDocumentQuery igniteQuery) {
		/*
		 * The parser converts results from Ignite
		 * queries to vertices.
		 */
		DocumentReader parser = getReader();

		List<IgniteResult> it = igniteQuery.getSqlResultWithMeta(null);

		return IgniteResultTransform
				.map(it.iterator(),parser::parse);
	}

	/**
     * This method retrieves all vertices that refer to
     * the same label.
     */
    public Iterator<Vertex> verticesQuery(String sql, Object[] args) {
    	List<IgniteResult> it = this.getTable().getIgniteSqlQuery(sql).getSqlResultWithMeta(args);
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
    	DocumentReader parser = getReader();       

        return IgniteResultTransform
                .map(it.iterator(),parser::parse);
    }

	
}
