package de.kp.works.ignite.graph;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import de.kp.works.ignite.ValueType;
import de.kp.works.ignite.ValueUtils;
import de.kp.works.ignite.graph.IgniteEdgeEntry;
import de.kp.works.ignite.graph.IgniteVertexEntry;

import static de.kp.works.ignite.ValueType.DATE;
import static de.kp.works.ignite.ValueType.STRING;

public class VertexEntryIterator implements CloseableIterator<IgniteVertexEntry>,AutoCloseable {
	private Iterator<List<?>> sqlResult;
	private FieldsQueryCursor<List<?>> cursor;
	
	public VertexEntryIterator(FieldsQueryCursor<List<?>> curser) {
		this.cursor = curser;
		this.sqlResult = curser.iterator();
	}
	
	public static List<IgniteVertexEntry> toList(Iterator<IgniteVertexEntry> it) {
		ArrayList<IgniteVertexEntry> list = new ArrayList<IgniteVertexEntry>();
		while(it.hasNext()) {
			list.add(it.next());
		}
		return list;
	}
	
	public <T> CloseableIterator<T> map(Function<IgniteVertexEntry,T> func){
		return new CloseableIterator<T>() {			
			@Override
			public boolean hasNext() {				
				return sqlResult.hasNext();
			}

			@Override
			public T next() {
				T newObj = func.apply(VertexEntryIterator.this.next());
				return newObj;
			}
			
			@Override
		    public void close() {
				VertexEntryIterator.this.close();
		    }
			
		};
	}
	
	public Stream<IgniteVertexEntry> stream(){
		return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.ORDERED)
                , false);
	}
	
	@Override
    public void close() {
		if(this.cursor!=null) {
			cursor.close();
		 	cursor = null;
		}
    }
	
	@Override
	public boolean hasNext() {		
		return sqlResult.hasNext();
	}
	
	/*
     * 0 : Cache key
     *
     * 1 : IgniteConstants.ID_COL_NAME (String)
     * 2 : IgniteConstants.ID_TYPE_COL_NAME (Short)
     * 3 : IgniteConstants.LABEL_COL_NAME (String)
     * 4 : IgniteConstants.CREATED_AT_COL_NAME (Long)
     * 5 : IgniteConstants.UPDATED_AT_COL_NAME (Long)
     * 6 : IgniteConstants.PROPERTY_KEY_COL_NAME (String)
     * 7 : IgniteConstants.PROPERTY_TYPE_COL_NAME (Short)
     * 8 : IgniteConstants.PROPERTY_VALUE_COL_NAME (String)
     */
	@Override
	public IgniteVertexEntry next() {
		List<?> result = sqlResult.next();

		String cacheKey = (String) result.get(0);

		String id = (String) result.get(1);
		short idType = (Short) result.get(2);

		String label = (String) result.get(3);

		Long createdAt = (Long) result.get(4);
		Long updatedAt = (Long) result.get(5);

		String propKey = (String) result.get(6);
		short propType = (Short) result.get(7);
		Object propValue = result.get(8);

		if (propType != STRING.code()) {
			propValue = ValueUtils.parseValue(propValue.toString(), ValueType.valueOf(propType));
		}

		return new IgniteVertexEntry(
				id,
				idType,
				label,
				createdAt,
				updatedAt,
				propKey,
				propType,
				propValue);

	}
}
