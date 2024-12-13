package de.bwaldvogel.mongo.backend.ignite;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.bson.Document;

import com.github.vincentrussell.query.mongodb.sql.converter.MongoDBQueryHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;
import com.github.vincentrussell.query.mongodb.sql.converter.SQLCommandType;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.from.SQLCommandInfoHolder;
import com.google.common.collect.Iterables;

import de.bwaldvogel.mongo.backend.ArrayFilters;
import de.bwaldvogel.mongo.backend.CloseableIterator;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.QueryParameters;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.CloseableIterator.DefaultCloseableIterator;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;


public class SQLQueryRunner {
	
	static Field documentAsMap = null;
	{
		try {
			documentAsMap = org.bson.Document.class.getDeclaredField("documentAsMap");
			documentAsMap.setAccessible(true);
		} catch (NoSuchFieldException | SecurityException e) {				
			e.printStackTrace();
		}
	}
	
	final QueryConverter queryConverter;
	
	final SQLCommandInfoHolder sqlCommandInfoHolder;
	
	private final Integer aggregationBatchSize;
    private final Boolean aggregationAllowDiskUse;
    
    public static de.bwaldvogel.mongo.bson.Document _Doc(org.bson.Document doc) {
    	if(doc==null) {
    		return null;
    	}    	
    	try {
    		de.bwaldvogel.mongo.bson.Document doc2 = new de.bwaldvogel.mongo.bson.Document((Map)documentAsMap.get(doc));
    		for(Entry<String,Object> kv: doc2.entrySet()) {
    			if(kv.getValue() instanceof Document) {
    				kv.setValue(_Doc((Document)kv.getValue()));
    			}
    			else if(kv.getValue() instanceof List) {
    				List<Object> list = (List) kv.getValue();
    				for(int i =0;i<list.size();i++) {
    					Object item = list.get(i);
    					if(item instanceof Document) {
    						de.bwaldvogel.mongo.bson.Document doc3 = _Doc((Document)item);
    						list.set(i, doc3);
    					}
    				}    				
    			}
    		}
    		return doc2;
		} catch (IllegalArgumentException | IllegalAccessException e) {
			return new de.bwaldvogel.mongo.bson.Document(doc);
		}
    }
    
    public static de.bwaldvogel.mongo.bson.Document _Doc(List list) {
    	if(list==null) {
    		return null;
    	}    	
    	int i = 0;
		de.bwaldvogel.mongo.bson.Document doc2 = new de.bwaldvogel.mongo.bson.Document();
		for(Object v: list) {
			if(v instanceof Document) {
				doc2.put(String.valueOf(i),_Doc((Document)v));
			}
			else if(v instanceof String){
				doc2.put(v.toString(),1);
			}
			else {
				doc2.put(String.valueOf(i),v);
			}
		}
		return doc2;
    }
    
    public static List<de.bwaldvogel.mongo.bson.Document> _Docs(List<org.bson.Document> docs) {
    	List<de.bwaldvogel.mongo.bson.Document> list = new ArrayList<>(docs.size());
    	docs.forEach(e-> list.add(_Doc(e)));
    	return list;
    }
	

    public SQLQueryRunner(QueryConverter queryConverter) {
		super();
		this.queryConverter = queryConverter;
		this.sqlCommandInfoHolder = queryConverter.getSqlCommandInfoHolder();
		this.aggregationBatchSize = queryConverter.aggregationBatchSize;
		this.aggregationAllowDiskUse = queryConverter.aggregationAllowDiskUse;
	}


	/**
     * @param mongoDatabase the database to run the query against.
     * @param <T>           variable based on the type of query run.
     * @return When query does a find will return QueryResultIterator&lt;{@link org.bson.Document}&gt;
     * When query does a count will return a Long
     * When query does a distinct will return QueryResultIterator&lt;{@link java.lang.String}&gt;
     * @throws ParseException when the sql query cannot be parsed
     */
    @SuppressWarnings("unchecked")
    public CloseableIterator<de.bwaldvogel.mongo.bson.Document> run(final IgniteDatabase mongoDatabase) throws ParseException {
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();

        IgniteBinaryCollection mongoCollection = (IgniteBinaryCollection)mongoDatabase.resolveCollection(mongoDBQueryHolder.getCollection(),true);

        if (SQLCommandType.SELECT.equals(mongoDBQueryHolder.getSqlCommandType())) {

            if (mongoDBQueryHolder.isDistinct()) {                
                String key = getDistinctFieldName(mongoDBQueryHolder);
                Document filter = mongoDBQueryHolder.getQuery();
                Set<de.bwaldvogel.mongo.bson.Document> values = new TreeSet<>(ValueComparator.ascWithoutListHandling());
                QueryResult result = mongoCollection.queryDocuments(_Doc(filter), null, 
                		(int) mongoDBQueryHolder.getOffset(),
            			(int) mongoDBQueryHolder.getLimit(), aggregationBatchSize, _Doc(mongoDBQueryHolder.getProjection()));
                for (de.bwaldvogel.mongo.bson.Document document : result) {
                    Object value = Utils.getSubdocumentValueCollectionAware(document, key);
                    if (!(value instanceof Missing)) {
                    	if (value instanceof de.bwaldvogel.mongo.bson.Document) {
                    		values.add((de.bwaldvogel.mongo.bson.Document)value);
                        } else if (value instanceof Collection) {
                        	for(Object v: (Collection)value) {
                        		de.bwaldvogel.mongo.bson.Document doc = new de.bwaldvogel.mongo.bson.Document(key,v);
                                values.add(doc);
                        	}                        	
                        } else {
                        	de.bwaldvogel.mongo.bson.Document doc = new de.bwaldvogel.mongo.bson.Document(key,value);
                            values.add(doc);
                        }
                    }
                }
                return new DefaultCloseableIterator<>(values.iterator());
                
            } else if (sqlCommandInfoHolder.isCountAll() && !queryConverter.isAggregate(mongoDBQueryHolder)) {
            	int n = mongoCollection.count(
            			_Doc(mongoDBQueryHolder.getQuery()),
            			(int) mongoDBQueryHolder.getOffset(),
            			(int) mongoDBQueryHolder.getLimit()
            			);
            	de.bwaldvogel.mongo.bson.Document doc = new de.bwaldvogel.mongo.bson.Document("n",n);
                return CloseableIterator.singleton(doc);
                
            } else if (queryConverter.isAggregate(mongoDBQueryHolder)) {
            	List<Document> steps = queryConverter.generateAggSteps(mongoDBQueryHolder, sqlCommandInfoHolder);
            	Document aggQuery = new Document();
                aggQuery.append("query",mongoDBQueryHolder.getQuery());

                Document cursor = new Document();
                cursor.append("limit",(int) mongoDBQueryHolder.getLimit());
                cursor.append("skip",(int) mongoDBQueryHolder.getOffset());
                cursor.append("batchSize",aggregationBatchSize);
                cursor.append("allowDiskUse",aggregationAllowDiskUse);
                aggQuery.append("cursor",cursor);
            	
            	Aggregation aggregation = mongoDatabase.getAggregation(_Docs(steps), _Doc(aggQuery), IgniteBackend.instance::resolveDatabase, mongoCollection, IgniteBackend.instance.getOplog());
            	
            	return aggregation.computeResult();
               
            } else {            	
            	QueryParameters queryParam = new QueryParameters(_Doc(mongoDBQueryHolder.getQuery()),
            			(int) mongoDBQueryHolder.getOffset(),
            			(int) mongoDBQueryHolder.getLimit(),
            			aggregationBatchSize,
            			_Doc(mongoDBQueryHolder.getProjection()),
            			_Doc(mongoDBQueryHolder.getSort())
            			);
            	
            	QueryResult result = mongoCollection.handleQuery(queryParam);               
            	return CloseableIterator.of(result.iterator());
            }
        } else if (SQLCommandType.DELETE.equals(mongoDBQueryHolder.getSqlCommandType())) {
            int deleteResult = mongoCollection.deleteDocuments(_Doc(mongoDBQueryHolder.getQuery()),(int) mongoDBQueryHolder.getLimit(),IgniteBackend.instance.getOplog());
            de.bwaldvogel.mongo.bson.Document doc = new de.bwaldvogel.mongo.bson.Document("n",deleteResult);
            return CloseableIterator.singleton(doc);
        } else if (SQLCommandType.INSERT.equals(mongoDBQueryHolder.getSqlCommandType())) {
        	List<Document> documents = List.of(mongoDBQueryHolder.getUpdateSet());
            de.bwaldvogel.mongo.bson.Document insertResult = mongoCollection.insertDocuments(_Docs(documents), true);            
            return CloseableIterator.singleton(insertResult);
        } else if (SQLCommandType.UPDATE.equals(mongoDBQueryHolder.getSqlCommandType())) {
            Document updateSet = mongoDBQueryHolder.getUpdateSet();
            List<String> fieldsToUnset = mongoDBQueryHolder.getFieldsToUnset();
            de.bwaldvogel.mongo.bson.Document updateObj = _Doc(updateSet);
            de.bwaldvogel.mongo.bson.Document selector = _Doc(mongoDBQueryHolder.getQuery());
            de.bwaldvogel.mongo.bson.Document update = null;
            
            boolean multi = true;
            boolean upsert = false;
            de.bwaldvogel.mongo.bson.Document result = null;
            if ((updateSet != null && !updateSet.isEmpty()) && (fieldsToUnset != null && !fieldsToUnset.isEmpty())) {
            	update = new de.bwaldvogel.mongo.bson.Document();
            	update.append("$set", updateObj);
            	update.append("$unset", _Doc(fieldsToUnset));
            	ArrayFilters arrayFilters = ArrayFilters.parse(updateObj, update);
                result = mongoCollection.updateDocuments(selector,update,arrayFilters,
                        multi,upsert,IgniteBackend.instance.getOplog());
            } else if (updateSet != null && !updateSet.isEmpty()) {
            	update = new de.bwaldvogel.mongo.bson.Document().append("$set", updateObj);
            	ArrayFilters arrayFilters = ArrayFilters.parse(updateObj, update);
                result = mongoCollection.updateDocuments(selector,update,arrayFilters,
                        multi,upsert,IgniteBackend.instance.getOplog());
            } else if (fieldsToUnset != null && !fieldsToUnset.isEmpty()) {
            	update = new de.bwaldvogel.mongo.bson.Document().append("$unset", _Doc(fieldsToUnset));
            	ArrayFilters arrayFilters = ArrayFilters.parse(updateObj, update);
                result = mongoCollection.updateDocuments(selector,update,arrayFilters,
                        multi,upsert,IgniteBackend.instance.getOplog());
            }
            return CloseableIterator.singleton(result);
        } else if (SQLCommandType.UPSERT.equals(mongoDBQueryHolder.getSqlCommandType())) {
            Document updateSet = mongoDBQueryHolder.getUpdateSet();            
            de.bwaldvogel.mongo.bson.Document updateObj = _Doc(updateSet);
            de.bwaldvogel.mongo.bson.Document selector = _Doc(mongoDBQueryHolder.getQuery());
            de.bwaldvogel.mongo.bson.Document update = new de.bwaldvogel.mongo.bson.Document();
            
            boolean multi = false;
            boolean upsert = true;
            de.bwaldvogel.mongo.bson.Document result = null;
            if ((updateSet != null && !updateSet.isEmpty())) {
            	update = new de.bwaldvogel.mongo.bson.Document();
            	if(mongoDBQueryHolder.getGroupBys().isEmpty()) {
            		update.append("$set", updateObj);
            	}
            	else {
            		update.append("$setOnInsert", updateObj);
            		for(String field: mongoDBQueryHolder.getGroupBys()) {
            			update.append("$set", new de.bwaldvogel.mongo.bson.Document(field,updateObj.get(field)));
            		}
            	}
            	         	
            	ArrayFilters arrayFilters = ArrayFilters.parse(updateObj, update);
                result = mongoCollection.updateDocuments(selector,update,arrayFilters,
                        multi,upsert,IgniteBackend.instance.getOplog());
            }
            return CloseableIterator.singleton(result);
        } else {
            throw new UnsupportedOperationException("SQL command type not supported");
        }
    }
    
    private String getDistinctFieldName(final MongoDBQueryHolder mongoDBQueryHolder) {
        return Iterables.get(mongoDBQueryHolder.getProjection().keySet(), 0);
    }	
}



