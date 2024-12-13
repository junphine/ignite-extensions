package de.bwaldvogel.mongo.backend.ignite.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;

import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.QueryFilter;
import de.bwaldvogel.mongo.bson.Document;

public class BinaryObjectMatch extends DefaultQueryMatcher implements IgniteBiPredicate<Object, Object>{
	private static final long serialVersionUID = 1L;	
	
	private final Document query;
	private final String idField;	
	
	public BinaryObjectMatch(Document query, String idField) {		
		this.query = query;
		this.idField = idField;
	}

	@Override 
	public boolean apply(Object keyValue, Object value) {
		if(value instanceof BinaryObject) {
			for (Map.Entry<String, Object> kv : query.entrySet()) {
	            Object queryValue = kv.getValue();
	            String key = kv.getKey();
	            validateQueryValue(queryValue, key);
				if (!this.checkMatch(queryValue,key,(BinaryObject)value)) {
					return false;
		        }
			}			
		}
     	return true;
     }
	
	private boolean checkMatch(Object queryValue, String key, BinaryObject document) {
        return checkMatch(queryValue, splitKey(key), document);
    }
	
	/**
	 * 预过滤，不确定的情况下返回true
	 * @param document
	 * @param query
	 * @return
	 */
    public boolean checkMatch(Object queryValue,List<String> keys, BinaryObject document) {       
        Object documentValue = null; 

        String key = keys.get(0);

        List<String> subKeys = Collections.emptyList();
        if (keys.size() > 1) {
            subKeys = keys.subList(1, keys.size());
        }
        
        if(keys.size()!=1) {
        	Object embedDocumentValue = document.field(keys.get(0));
        	for(String field: keys.subList(1,keys.size())) {
        		 embedDocumentValue = getFieldValueListSafe(embedDocumentValue,field);
        	}            	
        	documentValue = embedDocumentValue;
        }
        else {
            if (QueryFilter.isQueryFilter(key)) {
            	return true;
            }
            if (key.startsWith("$")) {
            	return true;
            }
            if(!document.hasField(key)) {
            	return true;
            }
            documentValue = document.field(key);
        }
        
        if (documentValue instanceof Collection<?>) {
            Collection<?> documentValues = (Collection<?>) documentValue;
            if (queryValue instanceof Document) {
                Document queryDocument = (Document) queryValue;
                boolean matches = checkMatchesAnyValue(queryDocument, keys, null, documentValues);
                if (matches) {
                	return true;
                }                    
                if (isInQuery(queryDocument)) {
                	matches = checkMatchesValue(queryDocument, documentValue);
                	if (matches) {
                		return true;
                    }
                	return false;
                } else {
                    return false;
                }
            } else if (queryValue instanceof Collection<?>) {
            	boolean matches = checkMatchesValue(queryValue, documentValues);
            	if (matches) {
            		return true;
                }
            	return false;
            } else if (!checkMatchesAnyValue(queryValue, documentValues)) {
                return false;
            }
        }
        else if (documentValue instanceof Map && !(documentValue instanceof Document)) {
        	documentValue = new Document((Map)documentValue);
        }

        return checkMatchesValue(queryValue, documentValue);
       
    }
    
    public static Object getFieldValueListSafe(Object value, String field) throws IllegalArgumentException {
        if (Missing.isNullOrMissing(value)) {
            return Missing.getInstance();
        }

        if (field.equals("$") || field.contains(".")) {
            throw new IllegalArgumentException("illegal field: " + field);
        }

        if (value instanceof List<?>) {
            List<?> list = (List<?>) value;
            if (isNumeric(field)) {
                int pos = Integer.parseInt(field);
                if (pos >= 0 && pos < list.size()) {
                    return list.get(pos);
                } else {
                    return Missing.getInstance();
                }
            } else {
                List<Object> values = new ArrayList<>();
                for (Object subValue : list) {
                    if (subValue instanceof Document) {
                        Object subDocumentValue = ((Document) subValue).getOrMissing(field);
                        if (!(subDocumentValue instanceof Missing)) {
                            values.add(subDocumentValue);
                        }
                    }
                }
                if (values.isEmpty()) {
                    return Missing.getInstance();
                }
                return values;
            }
        } else if (value instanceof Map) {
        	Map document = (Map) value;
            return document.get(field);
        } else {
            return Missing.getInstance();
        }
    }
    
    private static boolean isNumeric(String value) {
        return value.chars().allMatch(Character::isDigit);
    }

}
