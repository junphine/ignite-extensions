package de.bwaldvogel.mongo.backend.ignite.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.BinaryFieldReader;
import org.apache.ignite.internal.binary.BinaryArray;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;

import org.apache.ignite.internal.processors.igfs.IgfsBaseBlockKey;

import de.bwaldvogel.mongo.bson.ObjectId;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.lucene.util.BytesRef;

import com.fasterxml.jackson.databind.util.LRUMap;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.Bson;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.json.JsonConverter;
import de.bwaldvogel.mongo.wire.BsonConstants;
import de.bwaldvogel.mongo.wire.bson.BsonEncoder;


public class DocumentUtil {
	
	public static LRUMap<BytesRef,Object> keyDict = new LRUMap<>(100,5000);
	
	/**
	 *  将对象在类里面的公开字段作为key，生成document
	 * @param doc
	 * @param instance
	 * @param cls
	 * @return
	 */
	public static Document objectToDocumentForClass(Document doc, Object instance,Class<?> cls) {		
		//MapFactry x;		
		for(Field field : cls.getDeclaredFields()){
			if(!Modifier.isStatic(field.getModifiers())) {
				try {
	              Object result = null;
	              field.setAccessible(true);
	              result = field.get(instance);
	              if( result != null && !doc.containsKey(field.getName())) {
	            	  if(result instanceof IgniteUuid || result instanceof IgfsBaseBlockKey) {
	            		  result = result.toString();
	            	  }
	            	  try {
	            		  byte t = BsonEncoder.determineType(result);
	            		  if(t==BsonConstants.TYPE_EMBEDDED_DOCUMENT) {
	            			  final Document embed = new Document();
	            			  Map<String, Object> map = (Map)result;
	            			  map.forEach((k,v)->{
	            				try {
	            					byte t2 = BsonEncoder.determineType(v);
	            					embed.put(k.toString(),v);
	            				}
	            				catch(Exception e2) {
	            					Document json = toKeyValuePairs(v);
	      	            		  	json.append("_class", v.getClass().getName());
	      							embed.put(k.toString(),json);
	            				}
	      						
	      					  });
	            			  result = embed;
	            		  }
	            		  
	            		  doc.append(field.getName(), result); 
	            		  
	            	  }
	            	  catch(Exception e) {
	            		  Document json = new Document(result);
	            		  json.append("_class", result.getClass().getName());
	            		  doc.append(field.getName(), json);
	            	  }	            	 
	              }
	             
	            } catch (Exception e) {		             
	                e.printStackTrace();
	            }
			}
		}
		return doc;
	}
	/**
	 * 对非binary对象进行文档化
	 * @param instance
	 * @return
	 */
	public static Document toKeyValuePairs(Object instance) {		
		//MapFactry x;
		Document doc = new Document("_id",null);
		doc = objectToDocumentForClass(doc,instance,instance.getClass());
		Class<?> p = instance.getClass().getSuperclass();
		while(p!=null && p!=Object.class) {
			doc = objectToDocumentForClass(doc,instance,p);
			p = p.getSuperclass();
		}
		return doc;			 
	}
	
	public static Document objectToDocument(Object obj,String idField){		
		if(obj instanceof byte[] || obj instanceof Number || obj instanceof UUID || obj instanceof CharSequence || obj.getClass().isArray()) {
			
			Document doc = new Document(idField,null);			
			doc.append("_data", obj);
			return doc;
		}		
		else if(obj instanceof BinaryObject) {
			BinaryObject bobj = (BinaryObject) obj;
			Document doc =  binaryObjectToDocument(bobj);			
			return doc;
		}
		else if(obj instanceof Collection) {			
			Document doc = new Document(idField,null);			
			Collection coll = (Collection)obj;
			doc.append("_data", coll);
			return doc;
		}
		else if(obj instanceof Map) {			
			Map map = (Map)obj;			
			Document doc = new Document(map);						
			return doc;
		}
		else if(obj instanceof Vector) {
			Vector bobj = (Vector) obj;
			Document doc = new Document(idField,null);			
			doc.append("_data", bobj.getStorage().data());
			doc.append("_meta", new Document(bobj.getMetaStorage()));
			return doc;
		}
		else {							
			Document doc = toKeyValuePairs(obj);			
			return doc;
		}
		
	}

	
	public static Document objectToDocument(Object key,Object obj,String idField){		
		key = toDocumentKey(key,idField);						
		Document doc = objectToDocument(obj,idField);		
		doc.append(idField, key);
		return doc;
	}
	

	public static Object bsonObjectToJavaObject(Object $value){
		if($value instanceof List){
			List<Object> $arr = (List)$value;
			if(!$arr.isEmpty()) {
				Object item = $arr.get(0);
				if(item instanceof Number || item instanceof CharSequence) {
					
				}
				else if(item instanceof Document) {
					List<Object> list = new ArrayList<>($arr.size());
					for(int i=0;i<$arr.size();i++) {
						item = $arr.get(i);
						list.add(bsonObjectToJavaObject(item));
					}			
					$value = list;
				}
			}
			
		}
		else if($value instanceof Collection){
			Collection $arr = (Collection)$value;			
			$value = ($arr);
		}
		else if($value instanceof Document){
			Document doc = (Document)$value;					
			Map<String,Object> result = new LinkedHashMap<>(doc.size());
			Set<Map.Entry<String,Object>> ents = doc.entrySet();
		    for(Map.Entry<String,Object> ent: ents){	    	
		    	String $key =  ent.getKey();
		    	Object $val = bsonObjectToJavaObject(ent.getValue());		    	
		    	result.put($key, $val);
		    }
		    $value = result;
		}			
		else if($value instanceof BinData){
			BinData $arr = (BinData)$value;					
			$value = $arr.getData();
		}		
		return $value;
		
	}
	
    
	/**
	 * document key only support bson type
	 * @param key
	 * @param idField
	 * @return
	 */
	public static Object toDocumentKey(Object key,String idField) {
		if(key!=null) {
    		if(key instanceof BinaryObject){
				BinaryObject $arr = (BinaryObject)key;
				key = binaryObjectToDocumentOrPojo($arr,1);
				try {
					byte t2 = BsonEncoder.determineType(key);					
				}
				catch(Exception e) {
					byte[] buff = Base64.getEncoder().encode(key.toString().getBytes(StandardCharsets.UTF_8));
					keyDict.put(new BytesRef(buff), $arr);
					return buff;
				}
			}
    	}
	    return key;
	}
	
	public static Object toBinaryKey(Object key) {
		if (key == null) {
            return Missing.getInstance();
        }
		else if(key instanceof BinData){
			key = ((BinData)key).getData();
		}
		else if(key instanceof byte[]){
			Object bKey = keyDict.get(new BytesRef((byte[])key));
			if(bKey!=null) {
				return bKey;
			}
		}
		else if(key instanceof Number){
			key = Utils.normalizeNumber((Number)key);
		}
		else if(key instanceof ObjectId){
			key = ((ObjectId)key).getHexData();
		}
	    return key;
	}
	
	/**
	 * Binary decoder value only support bson type
	 * @param key
	 * @param idField
	 * @return
	 */
	public static Object toBsonValue(Object $value,int level) {		
		if($value!=null) {			
			if($value instanceof CharSequence || $value instanceof Number || $value instanceof UUID || $value instanceof Bson){
				return $value;
			}
			else if($value.getClass().isArray()) {
				if($value instanceof Object[]) {
					Object [] arr = (Object[])$value;
					if(arr.length>0) {
						if(arr[0] instanceof BinaryObject) {
							List<Object> $arr2 = new ArrayList<>(arr.length);
							for(int i=0;i< arr.length;i++) {							
								$arr2.add(toBsonValue(arr[i],level+1));
							}
							$value = $arr2;
						}
					}
				}
				return $value;
			}
			else if($value.getClass().isEnum()) {
				return ((Enum)$value).name();
			}
			else if($value instanceof List){
				List $arr = (List)$value;
				List<Object> $arr2 = new ArrayList<>($arr.size());
				for(int i=0;i<$arr.size();i++) {
					Object $valueSlice = $arr.get(i);
					
					$arr2.add(toBsonValue($valueSlice,level+1));
				}
				$value = $arr2;
			}
			else if($value instanceof Set){
				Set $arr = (Set)$value;
				Set<Object> $arr2 = new HashSet<>($arr.size());
				Iterator<Object> it = $arr.iterator();
				while(it.hasNext()) {
					Object $valueSlice = it.next();
					$arr2.add(toBsonValue($valueSlice,level+1));
				}
				$value = ($arr2);
			}
			else if($value instanceof Map){
				Map<Object, Object> $arr = (Map)$value;
				final Document docItem = new Document();
				for(Map.Entry<Object, Object> ent: $arr.entrySet()) {
					Object v = ent.getValue();
					docItem.put(ent.getKey().toString(), toBsonValue(v,level+1));
				}					
				$value = docItem;					
			}			
			else if($value instanceof BinaryObject){
				BinaryObject $arrSlice = (BinaryObject)$value;					
				$value = binaryObjectToDocumentOrPojo($arrSlice,level);
			}			
			else {				
				try {
					byte t2 = BsonEncoder.determineType($value);					
				}
				catch(Exception e) {
					Document json = toKeyValuePairs($value);
          		  	json.append("_class", $value.getClass().getName());
          		    $value = json;
				}
			}			
    	}
	    return $value;
	}


	/**
	 * top decode
	 * @param key
	 * @param obj
	 * @param idField document _id
	 * @return
	 */
	public static Document binaryObjectToDocument(BinaryObject obj){
		Document doc = null;
		Object $value = binaryObjectToDocumentOrPojo(obj,0);
		if($value instanceof Document) {
			doc = (Document)$value;			
		}
		else {
			doc = objectToDocument($value,"_id");
		}    	
	    return doc;
	}
	
    public static Object binaryObjectToDocumentOrPojo(BinaryObject bobj,int level){
    	Collection<String> fields = null;
    	try {    		
    		if(bobj instanceof BinaryObjectImpl) {
	    		BinaryObjectImpl bin = (BinaryObjectImpl)bobj;
	    		if(!bin.hasSchema()) {
	    			return toBsonValue(bin.deserialize(),level);
	    		}
    		}
    		else if(bobj instanceof BinaryArray) {
    			BinaryArray bin = (BinaryArray)bobj;
	    		if(bin.componentClassName().startsWith("java.")) {
	    			return bin.deserialize();
	    		}
	    		return bin.deserialize();
    		}
    		else if(bobj instanceof BinaryEnumObjectImpl) {
    			BinaryEnumObjectImpl bin = (BinaryEnumObjectImpl)bobj;	    		
	    		return ((Enum)bin.deserialize()).name();
    		}
    		
    		String typeName = bobj.type().typeName();
    		if(typeName.equals("Document") || typeName.equals("SerializationProxy")) {
    			return bobj.deserialize();
    		}
    		
    		fields = bobj.type().fieldNames();
    		if(fields==null || fields.size()<=1) {
    			return toBsonValue(bobj.deserialize(),level);
    		}    		
    	}
    	catch(BinaryObjectException e) {
    		if(bobj instanceof BinaryEnumObjectImpl) {
    			BinaryEnumObjectImpl bin = (BinaryEnumObjectImpl)bobj;	    		
	    		return bin.enumName();
    		}
    		fields = bobj.type().fieldNames();	
    	}
    	
    	Document doc = level==0? new Document("_id",null) : new Document();
	    for(String field: fields){	    	
	    	String $key =  field;
	    	Object $value = bobj.field(field);
			try {				
				if($value!=null) {					
					doc.append($key, toBsonValue($value,level+1));
				}
				else {
					doc.append($key, null);
				}
				
			} catch (Exception e) {				
				e.printStackTrace();
			}	    	
	    }
	    return doc;
	}
   
    /**
     * 
     * @param igniteBinary
     * @param keyValue
     * @param obj
     * @param typeName 
     * @param keyField BinaryObject id 
     * @return
     */
    public static BinaryObject documentToBinaryObject(IgniteBinary igniteBinary,String typeName, Document doc, String idField){	
    	String docTypeName = (String)doc.getOrDefault("_class",typeName);	
    	BinaryTypeImpl type = (BinaryTypeImpl)igniteBinary.type(docTypeName);
		BinaryObjectBuilder bb = igniteBinary.builder(docTypeName);	
		Set<Map.Entry<String,Object>> ents = doc.entrySet();
	    for(Map.Entry<String,Object> ent: ents){	    	
	    	String $key =  ent.getKey();
	    	Object $value = ent.getValue();
	    	
			try {	
				$value = bsonObjectToJavaObject($value);
			
				if(type!=null && $value instanceof Number) {
					BinaryFieldMetadata field = type.metadata().fieldsMap().get($key);
					if(field!=null) {
						$value = BinaryFieldReader.readNumberBinaryField((Number)$value,field.typeId());
					}
					bb.setField($key, $value);
					continue;
				}
				else if(type!=null) {
					BinaryFieldMetadata field = type.metadata().fieldsMap().get($key);
					if(field!=null) {
						$value = BinaryFieldReader.readOtherBinaryField($value,field.typeId());
					}
				}
				if($key.equals(idField) && doc.size()>1) {
								
				}
				else if($key.equals("_data") && doc.size()<=2) {
					Object bValue = igniteBinary.toBinary($value);
					if(bValue instanceof BinaryObject) {						
						return (BinaryObject)bValue;
					}
					else {
						bb.setField($key, $value);
					}
				}				
				else if($value!=null) {
					Object bValue = igniteBinary.toBinary($value);
					bb.setField($key, bValue);
				}
				else {
					bb.setField($key, null);
				}				
				
			} catch (Exception e) {				
				e.printStackTrace();
			}	    	
	    }
	    
	    BinaryObject bobj = bb.build();
	    return bobj;
	}
}
