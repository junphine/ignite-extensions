package de.bwaldvogel.mongo.backend.ignite;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.cache.Cache;
import javax.cache.Cache.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.predicate.FieldEqualsMatch;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;
import com.google.common.collect.Sets;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.CloseableIterator;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.ComposeKeyValue;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ignite.util.BinaryObjectMatch;
import de.bwaldvogel.mongo.backend.ignite.util.TransformerUtil;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.IllegalOperationException;
import de.bwaldvogel.mongo.exception.InvalidOptionsException;
import de.bwaldvogel.mongo.exception.TypeMismatchException;


import static de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil.*;

public class IgniteBinaryCollection extends AbstractMongoCollection<Object> {

	final IgniteCache<Object, BinaryObject> dataMap;	
	final String idField;
	private String tableName;
	private String typeName;
	private boolean readOnly = false;
    
    public IgniteBinaryCollection(IgniteDatabase database, String collectionName, CollectionOptions options,
            CursorRegistry cursorRegistry, IgniteCache<Object, BinaryObject> dataMap) {
        super(database, collectionName,options,cursorRegistry);
        this.dataMap = dataMap;
        this.idField = options.getIdField();
        
        if(collectionName.startsWith("igfs-internal-")) { // igfs
        	this.readOnly = true;
        }
        if(collectionName.startsWith("wc_")) { // web-console
        	this.readOnly = true;
        }
    }
    

    @Override
    public void addIndex(Index<Object> index) {
    	if(index==null) return; //add@byron
    	super.addIndex(index);
    	indexChanged(index,"add");
    }    
   
    protected void indexChanged(Index<Object> index,String op) {
    	boolean firstLuneceIndex = true;
    	for (Index<Object> idx : this.getIndexes()) {
			if (idx instanceof IgniteLuceneIndex) {
				IgniteLuceneIndex igniteIndex = (IgniteLuceneIndex) idx;
				if(op.equals("reIndex"))
					igniteIndex.init(this);
				if(op.equals("add")) {
					igniteIndex.setFirstIndex(firstLuneceIndex);
					firstLuneceIndex = false;
				}
			}
			if (idx instanceof IgniteVectorIndex) {
				IgniteVectorIndex igniteIndex = (IgniteVectorIndex) idx;
				if(op.equals("reIndex"))
					igniteIndex.init(this);
			}
    	}
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
    	
    }
    
    protected boolean tracksDataSize() {
        return false;
    }

    @Override
    protected int getDataSize() {
    	int size = (int)dataMap.metrics().getCacheSize();
        return size;
    }


    @Override
    protected Object addDocumentInternal(Document document) {
    	if(readOnly) {
    		throw new IllegalOperationException("This collection is read only!");
    	}
        Object key = null;
        if (idField != null) {
            key = document.get(idField);
        }
        if (key == null && idField != "_id") {
            key = document.get("_id");
        }
        
        if(key==null || key==Missing.getInstance()) {
            key = UUID.randomUUID();
        }        
        
    	String typeName = this.getTypeName();	
    	
    	IgniteDatabase database = (IgniteDatabase)this.getDatabase();
    	try {
    		key = toBinaryKey(key);
    		BinaryObject obj = documentToBinaryObject(database.getIgnite().binary(),typeName,document,idField);
    		boolean rv = dataMap.putIfAbsent(key, obj);
            if(!rv) {
            	throw new DuplicateKeyError(this.getCollectionName(),"Document with key '" + key + "' already existed");
            }
            //Assert.isNull(previous, () -> "Document with key '" + key + "' already existed in " + this + ": " + previous);
            return key;
    	}
    	catch(BinaryObjectException e) {
    		throw new TypeMismatchException(e.getMessage());
    	}
    	catch(IgniteException e) {
    		throw new BadValueException(e.getMessage());
    	}
       
        
    }

    @Override
    public int count() {
    	if(this.getCollectionName().equals("system.namespaces")) {
    		IgniteDatabase database = (IgniteDatabase)this.getDatabase();
    		return ((List)database.listCollectionNamespaces()).size();
    	}
        return dataMap.size();
    }

    @Override
    protected Document getDocument(Object position) {
    	// position with score
    	if(position instanceof IdWithMeta) { // _key,_score,_indexValue
    		IdWithMeta idWithScore = (IdWithMeta)position;    		
    		position = idWithScore.key;    		
    		Object obj = dataMap.get(position);
        	if(obj==null) return null;
        	Document doc = objectToDocument(position,obj,idField);
        	Document meta = (Document) doc.computeIfAbsent("_meta", (k)-> idWithScore.meta);
        	if(meta!=null && idWithScore.meta!=null && idWithScore.meta!=meta) {
        		meta.putAll(idWithScore.meta);
        	}
        	
        	return doc;
    	}
    	Object obj = dataMap.get(position);
    	if(obj==null) return null;
    	return objectToDocument(position,obj,idField);
    }

    @Override
    protected void removeDocument(Object position) {
    	if(readOnly) {
    		throw new IllegalOperationException("This collection is read only!");
    	}
        boolean remove = dataMap.remove(position);
        if (!remove) {
            throw new NoSuchElementException("No document with key " + position);
        }
    }

    @Override
    protected Object findDocumentPosition(Document document) {
    	 Object key = document.getOrDefault(this.idField, null);
    	 if(key!=null) {
    		 key = toBinaryKey(key);
    		 return key;
    	 }    	 
         return null;
    }
    
    @Override
    protected QueryResult queryDocuments(Document query, Document orderBy, int numberToSkip, int limit, int batchSize,
            Document fieldSelector) {
		query = Utils.normalizeDocument(query);
		
		List<Iterable<Object>> indexResult = new ArrayList<>();
		
		List<Index<Object>> enabledIndexes = new ArrayList<>();
		for (Index<Object> index : getIndexes()) {
			if (index.canHandle(query)) {
				enabledIndexes.add(index);
			}
		}
		
		Collections.sort(enabledIndexes,TransformerUtil::indexCompareTo);
		
		for (Index<Object> index : enabledIndexes) {
			Iterable<Object> positions = index.getPositions(query);
			if(index.isUnique() || (positions instanceof List && ((List)positions).size()<=100)) {
				return matchDocuments(query, positions, orderBy, numberToSkip, limit, batchSize, fieldSelector);
			}
			else {
				indexResult.add(positions);
			}
		}
		if(indexResult.size()==1) {
			return matchDocuments(query, indexResult.get(0), orderBy, numberToSkip, limit, batchSize, fieldSelector);
		}
		else if(indexResult.size()>1) {			
			final LinkedHashMap<Object,Object> resultMap = new LinkedHashMap<>();
			HashSet<Object> ids = new HashSet<>();
			int n = 0;
			for(Iterable<Object> positions: indexResult) {
				for(Object position: positions) {
					Object rawPosition = position;
					if(position instanceof IdWithMeta) {
						IdWithMeta idWithScore = (IdWithMeta)position;    		
			    		rawPosition = idWithScore.key;				    		
					}
					
					if(n==0) {
						resultMap.put(rawPosition, position);
					}
					else {
						Object originPosition = resultMap.get(rawPosition);
						if(originPosition!=null) {
							if(position instanceof IdWithMeta) {
								IdWithMeta idWithScore = (IdWithMeta)position;  
								if(originPosition instanceof IdWithMeta) {
									IdWithMeta idWithScoreOrigin = (IdWithMeta)originPosition;
									if(idWithScoreOrigin.meta!=null) {
										if(idWithScore.meta!=null)
											idWithScoreOrigin.meta.putAll(idWithScore.meta);
									}
									else {
										resultMap.put(rawPosition, position);
									}
								}
								else {
									resultMap.put(rawPosition, position);
								}
								
							}
							ids.add(rawPosition);
						}
					}						
				}					
				
				if(n>0) {
					Set<Object> removes = Sets.difference(resultMap.keySet(),ids);
					Sets.newCopyOnWriteArraySet(removes).forEach(id->{
						resultMap.remove(id);
					});
					ids.clear();
				}					
				n++;
			}
			
			return matchDocuments(query, resultMap.values(), orderBy, numberToSkip, limit, batchSize, fieldSelector);
		}
		
		return matchDocuments(query, orderBy, numberToSkip, limit, batchSize, fieldSelector);
	}


    @Override
	protected QueryResult matchDocuments(Document query, Document orderBy, 
			int numberToSkip, int numberToReturn,	int batchSize, Document fieldSelector) {
    	CloseableIterator<Document> list = this.matchDocuments(query, orderBy, numberToSkip, numberToReturn);
    	Stream<Document> documentStream = StreamSupport.stream(list.toSpliterators(), false);
    	documentStream.onClose(list::close);
    	return matchDocumentsFromStream(documentStream, query, orderBy, numberToSkip, numberToReturn, batchSize, fieldSelector);
		
	}
    
    protected CloseableIterator<Document> matchDocuments(Document query, Document orderBy, int numberToSkip, int numberToReturn) {       
        boolean isSysDB = this.getDatabaseName().equals(IgniteDatabase.SYS_DB_NAME);
        if(query.containsKey("$sql")) {
        	Object sqlQuery = query.get("$sql");
        	SqlFieldsQuery sqlQ;
        	if(sqlQuery instanceof String) {
        		sqlQ = new SqlFieldsQuery(sqlQuery.toString());
        	}
        	else if(sqlQuery instanceof Document) {        		
        		Document sqlInfo = (Document)sqlQuery;
        		String sql = (String)sqlInfo.get("query");
        		sqlQ = new SqlFieldsQuery(sql);
        		if(sqlInfo.containsKey("args")) {
        			Object args = sqlInfo.get("args");
        			if(args instanceof List) {
        				sqlQ.setArgs(((List)args).toArray());
        			}
        			else {
        				sqlQ.setArgs(args);
        			}
        		}
        		if(sqlInfo.containsKey("schema")) {
        			sqlQ.setSchema(sqlInfo.get("schema").toString());
        		}
        		if(sqlInfo.containsKey("local")) {
        			sqlQ.setLocal(sqlInfo.get("local").equals(true));
        		}
        		if(sqlInfo.containsKey("distributedJoins")) {
        			sqlQ.setDistributedJoins(sqlInfo.get("distributedJoins").equals(true));
        		}
        		if(sqlInfo.containsKey("collocated")) {
        			sqlQ.setCollocated(sqlInfo.get("collocated").equals(true));
        		}
        	}
        	else {
        		throw new InvalidOptionsException("$sql value must be String or Document!");
        	}
       	 	
        	if(getTableName()!=null) {
        		// cache does not have table or queryentity
	        	FieldsQueryCursor<List<?>>  cursor = dataMap.query(sqlQ);
	    		
	    		return TransformerUtil.mapListField(cursor,this.idField);
       	 	}
        	else {
        		QueryConverter queryConverter;
				try {
					queryConverter = new QueryConverter.Builder()
							.sqlString(sqlQ.getSql())
							.aggregationBatchSize(sqlQ.getPageSize())
							.aggregationAllowDiskUse(false)
							.build();
					
					if(true) {
						ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
				        queryConverter.write(byteArrayOutputStream);
				        this.log.info(byteArrayOutputStream.toString("UTF-8"));
					}					
					
					SQLQueryRunner runner = new SQLQueryRunner(queryConverter);
					if(queryConverter.getMongoQuery().getCollection().toLowerCase().startsWith("information_schema.")) {
						return runner.run((IgniteDatabase)IgniteBackend.instance.adminDatabase());
			        }
					return runner.run((IgniteDatabase)this.getDatabase());
					
				} catch (ParseException e) {
					throw new FailedToParseException(e.getMessage());					
				} catch (IOException e) {
					throw new IllegalOperationException(e.getMessage());	
				}
        		
        	}
        }
        else {
	        IgniteBiPredicate<Object, Object> filter = null;
	        if(isSysDB && query.containsKey("ns")) {
	        	final String ns = (String)query.get("ns");
	        	if(ns!=null) {
		        	filter = new FieldEqualsMatch("ns",ns);
	        	}
	        }
	        else {
	        	filter = new BinaryObjectMatch(query,this.idField);
	        }
	        
	        ScanQuery<Object, Object> scan = new ScanQuery<>(query.isEmpty() ? null : filter);
		 
			QueryCursor<Cache.Entry<Object, Object>>  cursor = dataMap.query(scan);		  
		    
		    return TransformerUtil.map(cursor,this.idField);
        }
    }

    
    @Override
    public void drop() {
    	if(readOnly) {
    		throw new IllegalOperationException("This collection is read only!");
    	}    	
    	dataMap.clear();
    	if(!this.getCollectionName().startsWith("system.")) {    		
    		dataMap.destroy();
    	}
    }
    
    public void close() {
    	for(Index<Object> idx: this.getIndexes()) {
    		if (idx instanceof IgniteLuceneIndex) {
				IgniteLuceneIndex igniteIndex = (IgniteLuceneIndex) idx;				
				igniteIndex.close();
			}
			if (idx instanceof IgniteVectorIndex) {
				IgniteVectorIndex igniteIndex = (IgniteVectorIndex) idx;				
				igniteIndex.close();
			}
    	}
    }

    @Override
    protected void handleUpdate(Object key, Document oldDocument,Document document) {
    	if(readOnly) {
    		throw new IllegalOperationException("This collection is read only!");
    	}    	
    	String typeName =  this.getTypeName();
    	
    	IgniteDatabase database = (IgniteDatabase)this.getDatabase();
    	BinaryObject obj = documentToBinaryObject(database.getIgnite().binary(),typeName,document,idField);
        
        dataMap.put(toBinaryKey(key), obj);
    }


    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
    	// Get the data streamer reference and stream data.    	
    	
    	 ScanQuery<Object, BinaryObject> scan = new ScanQuery<>();
    		 
    	 QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
    	
    	 return StreamSupport.stream(cursor.spliterator(),false).map(entry -> new DocumentWithPosition<>(objectToDocument(entry.getKey(),entry.getValue(),this.idField), entry.getKey()));		
         
    }
   
    public String getTableName() {
    	if(tableName!=null) {
    		return tableName;
    	}
    	CacheConfiguration<Object,BinaryObject> cfg = dataMap.getConfiguration(CacheConfiguration.class);
    	String schema = cfg.getSqlSchema();
    	if(schema==null) {
    		schema = cfg.getName();
    	}
    	String cacheName = cfg.getName();
    	
    	IgniteDatabase db = (IgniteDatabase)this.getDatabase();
    	GridKernalContext ctx = ((IgniteEx)db.getIgnite()).context();
    	IgniteH2Indexing igniteH2Indexing = (IgniteH2Indexing) ctx.query().getIndexing();
    	if(igniteH2Indexing!=null) {
    		Collection<TableDescriptor> tables = igniteH2Indexing.schemaManager().tablesForCache(cacheName);
    		for(TableDescriptor table: tables) {    			
    			if (table.type().textIndex() != null) {
    				tableName = table.type().tableName();
    				typeName = table.type().name();
    				break;
    			}
    			if(tableName==null) {
    				tableName = table.type().tableName();
    				typeName = table.type().name();
    			}
    		}
    	}   	
    	// SQL_{SCHEMA_NAME}_{TABLE_NAME}	
		if(cacheName.startsWith("SQL_")) {
			String patten = "SQL_"+schema+"_";
			if(cacheName.startsWith(patten)) {
				tableName = cacheName.substring(patten.length());
			}
			else {
				int pos = cacheName.lastIndexOf('_',5);
				if(pos>0)
					tableName =  cacheName.substring(pos+1);
			}
		}
		return tableName;
	}
    /**
     * 
     * @param dataMap
     * @param obj
     * @return schema,typeName,keyField
     */
    public String getTypeName() {
    	String shortName = getTableName();
    	if(typeName!=null) {
    		return typeName;
    	}    	
    	if(!StringUtils.isEmpty(shortName)) {
    		int pos = shortName.lastIndexOf('.');
    		shortName = pos>0? shortName.substring(pos+1): shortName;
    	}    	
    	
    	CacheConfiguration<Object,BinaryObject> cfg = dataMap.getConfiguration(CacheConfiguration.class);    	
    	if(!cfg.getQueryEntities().isEmpty()) {
    		Iterator<QueryEntity> qeit = cfg.getQueryEntities().iterator();
    		while(qeit.hasNext()) {
	    		QueryEntity entity = qeit.next();
	    		if(StringUtils.isEmpty(typeName)) {
	        		typeName = entity.getValueType();	        		
	        	}
	    		else if(this.getCollectionName().equalsIgnoreCase(entity.getValueType()) || shortName.equalsIgnoreCase(entity.getTableName())){
	    			typeName = entity.getValueType();
	    			break;
	    		}	    		
    		}
    	}
    	
    	if(StringUtils.isEmpty(typeName)) {
    		// read type name from existed records
    		ScanQuery<Object, Object> scan = new ScanQuery<>(null);
    		// scan.setLocal(true);
    		scan.setPageSize(1);
			QueryCursor<Cache.Entry<Object, Object>>  cursor = dataMap.query(scan);
			for(Cache.Entry<Object, Object> row: cursor) {
				if(row.getValue() instanceof BinaryObject) {
					typeName = ((BinaryObject)row.getValue()).type().typeName();
				}
				else {
					typeName = row.getValue().getClass().getName();
				}
				break;
			}
			cursor.close();
    	}   
    	
    	return typeName==null ? this.getCollectionName() : typeName;
    }    
    
}
