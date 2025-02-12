package org.apache.ignite.internal.processors.query;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;

import org.jetbrains.annotations.Nullable;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoQuery;


/** No op implementation. */
public class MongoQueryEngine extends GridProcessorAdapter implements QueryEngine {
	
	// Map to hold SQL type to its corresponding wrapper class
    private static final Map<Integer, Class<?>> stringTypeToWrapperClass = new HashMap<>();
 
    static {
        
    	stringTypeToWrapperClass.put(Types.CHAR, String.class);
    	stringTypeToWrapperClass.put(Types.VARCHAR, String.class);
    	stringTypeToWrapperClass.put(Types.LONGVARCHAR, String.class);        
    	stringTypeToWrapperClass.put(Types.BINARY, byte[].class);
    	stringTypeToWrapperClass.put(Types.VARBINARY, byte[].class);
    	stringTypeToWrapperClass.put(Types.LONGVARBINARY, byte[].class);
        // Add more types if needed
    }
 
	private String database;
    /**
     * @param ctx Kernal context.
     */
    public MongoQueryEngine(GridKernalContext ctx) {
        super(ctx);
        this.database = ctx.igniteInstanceName();
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> query(
        @Nullable QueryContext ctx,
        String schemaName,
        String qry,
        Object... params
    ) throws IgniteSQLException {
    	Document sql = new Document("$sql",convertPrepareStatementToSql(qry,params));
    	MongoQuery mongoQuery = new MongoQuery(null,null,this.database+'.'+schemaName,0,-1,sql,null);
    	QueryResult result = IgniteBackend.instance.handleQuery(mongoQuery);
    	
    	List<GridQueryFieldMetadata> fieldsMeta = new ArrayList<>();
    	List<List<?>> resultList = new ArrayList<>();
    	for(Document doc: result) {
    		resultList.add(mapDoc(doc,fieldsMeta));
    	}
    	QueryCursorImpl<List<?>> queryCursor = new QueryCursorImpl<>(resultList);
    	queryCursor.fieldsMeta(fieldsMeta);
        return List.of(queryCursor);
    }

    /** {@inheritDoc} */
    @Override public List<List<GridQueryFieldMetadata>> parameterMetaData(
        @Nullable QueryContext ctx,
        String schemaName,
        String qry
    ) throws IgniteSQLException {
    	List<GridQueryFieldMetadata> metadata = new ArrayList<>();
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public List<List<GridQueryFieldMetadata>> resultSetMetaData(
        @Nullable QueryContext ctx,
        String schemaName,
        String qry
    ) throws IgniteSQLException {
    	Document sql = new Document("$sql",qry);
    	MongoQuery mongoQuery = new MongoQuery(null,null,this.database+'.'+schemaName,0,10,sql,null);
    	QueryResult result = IgniteBackend.instance.handleQuery(mongoQuery);
    	
    	List<GridQueryFieldMetadata> fieldsMeta = new ArrayList<>();
    	
    	for(Document doc: result) {
    		mapDoc(doc,fieldsMeta);
    	}
    	
        return List.of(fieldsMeta);
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> queryBatched(
        @Nullable QueryContext ctx,
        String schemaName,
        String qry,
        List<Object[]> batchedParams
    ) throws IgniteSQLException {
    	List<FieldsQueryCursor<List<?>>> cursors = new ArrayList<>();
    	for(Object[] params: batchedParams) {
	    	Document sql = new Document("$sql",convertPrepareStatementToSql(qry,params));
	    	MongoQuery mongoQuery = new MongoQuery(null,null,this.database+'.'+schemaName,0,-1,sql,null);
	    	QueryResult result = IgniteBackend.instance.handleQuery(mongoQuery);
	    	
	    	List<GridQueryFieldMetadata> fieldsMeta = new ArrayList<>();
	    	List<List<?>> resultList = new ArrayList<>();
	    	for(Document doc: result) {
	    		resultList.add(mapDoc(doc,fieldsMeta));
	    	}
	    	QueryCursorImpl<List<?>> queryCursor = new QueryCursorImpl<>(resultList);
	    	queryCursor.fieldsMeta(fieldsMeta);	    	
	    	cursors.add(queryCursor);
    	}
    	return cursors;
    }
    
    public static List<?> mapDoc(Document doc,List<GridQueryFieldMetadata> fieldsMeta){
    	int i = 0;
    	MongoFieldMetadata meta = null;
    	List<Object> row = new ArrayList<>(doc.size());
		for(Map.Entry<String,?> ent: doc.entrySet()) {
			row.add(ent.getValue());
			if(fieldsMeta.size()<=i) {
				meta = new MongoFieldMetadata();
				meta.name = ent.getKey();
				meta.type = ent.getValue().getClass().getSimpleName();
				if(ent.getValue()==null) {
					meta.nullability=1;
				}
				fieldsMeta.add(meta);
			}
			else {
				meta = (MongoFieldMetadata)fieldsMeta.get(i);
				if(ent.getValue()==null) {
					meta.nullability=1;
				}
			}
			i++;
		}
		return row;
	}
    
    
    /**
     * Converts a PreparedStatement SQL with placeholders to a SQL string with actual parameter values.
     *
     * @param sql     the SQL statement with placeholders (e.g., "SELECT * FROM users WHERE id = ?")
     * @param params  the parameter values
     * @return the SQL statement with actual parameter values
     */
    public static String convertPrepareStatementToSql(String sql, Object[] params) {
        if (params == null || params.length == 0) {
            return sql;
        }
	    StringBuilder result = new StringBuilder();
	    int paramIndex = 0;
	    int startIndex = 0;
	
	    while (true) {
	        int placeholderIndex = sql.indexOf("?", startIndex);
	        if (placeholderIndex == -1) {
	            result.append(sql.substring(startIndex));
	            break;
	        }
	
	        result.append(sql.substring(startIndex, placeholderIndex));
	
	        Object param = params[paramIndex++];
	        if (param == null) {
	            result.append("NULL");
	        } else {
	            Class<?> paramClass = param.getClass();
	            if (paramClass.isArray()) {
	                // Handle array types (e.g., int[], String[])
	                result.append(arrayToString(param));
	            } else if (stringTypeToWrapperClass.containsValue(paramClass)) {
	                // Handle primitive wrapper classes and String, etc.
	                result.append("'" + param.toString().replace("'", "''") + "'");
	            } else if (paramClass == java.sql.Date.class || paramClass == java.sql.Time.class || paramClass == java.sql.Timestamp.class) {
	                // Handle SQL date/time types
	                result.append("'" + param.toString() + "'");
	            } else {
	                // For other types, we assume they have a reasonable toString() method
	                result.append(param.toString());
	            }
	        }
	
	        startIndex = placeholderIndex + 1;
	    }
	
	    return result.toString();
    }
    
    private static String arrayToString(Object array) {
        StringBuilder sb = new StringBuilder();
        if (array.getClass().getComponentType() == byte.class) {
        	sb.append("'");
            byte[] byteArray = (byte[]) array;
            for (byte b : byteArray) {
                sb.append(String.format("0x%02X", b));
            }
            sb.append("'");
        }
        else if (array.getClass().getComponentType() == char.class) {
        	sb.append("'");
        	char[] byteArray = (char[]) array;
            for (char b : byteArray) {
                sb.append(String.format("0x%04X", b));
            }
            sb.append("'");
        }
        else {
	        sb.append("[");
	        if(array!=null) {
	            for (int i = 0; i < java.lang.reflect.Array.getLength(array); i++) {
	                Object element = java.lang.reflect.Array.get(array, i);
	                sb.append("'").append(element.toString().replace("'", "''")).append("', ");
	            }
	        }
	        if (sb.length() > 1) {
	            sb.setLength(sb.length() - 2); // Remove trailing ", "
	        }
	        sb.append("]");
        }
        return sb.toString();
    }
 
}
