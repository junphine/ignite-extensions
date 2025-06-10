package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.lang.IgniteBiTuple;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.bson.Bson;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;


public final class BinaryDocument implements Map<String, Object>, Bson {

    private static final long serialVersionUID = 1L;


    private final BinaryObjectImpl binaryObject;

    
    public BinaryDocument(BinaryObjectImpl object) {    	
    	binaryObject = object;
    }


    public BinaryDocument cloneDeeply() {
        return cloneDeeply(this);
    }

    @SuppressWarnings("unchecked")
    private static <T> T cloneDeeply(T object) {
        if (object == null) {
            return null;
        } else if (object instanceof BinaryDocument) {
            BinaryDocument document = (BinaryDocument) object;
            BinaryDocument clone = document.clone();            
            return (T) clone;
        } else if (object instanceof Map) {
        	Map document = (Map) object;
	        Document clone = new Document();
	        for (Object key : document.keySet()) {
	            clone.put(key.toString(), cloneDeeply(clone.get(key)));
	        }
	        return (T) clone;
	    }
        else if (object instanceof List) {
            List<?> list = (List<?>) object;
            List<?> result = list.stream()
                .map(BinaryDocument::cloneDeeply)
                .collect(Collectors.toList());
            return (T) result;
        } else if (object instanceof Set) {
            Set<?> set = (Set<?>) object;
            Set<?> result = set.stream()
                .map(BinaryDocument::cloneDeeply)
                .collect(Collectors.toCollection(LinkedHashSet::new));
            return (T) result;
        } else {
            return object;
        }
    }

    
    @Override
    public boolean containsValue(Object value) {    	
    	for(String field: this.binaryObject.type().fieldNames()) {
    		Object v = this.binaryObject.field(field);
    		if(value.equals(v)){
    			return true;
    		}
    	}    	
        return false;
    }

    @Override
    public Object get(Object key) {
        Object value = binaryObject.field(key.toString());
        return value;
    }

    // add@byron
    public BinaryDocument getBinaryDocument(Object key) {
        return new BinaryDocument((BinaryObjectImpl)binaryObject.field(key.toString()));
    }

    public Object getOrMissing(Object key) {
        return getOrDefault(key, Missing.getInstance());
    }

    @Override
    public void clear() {
    	throw new java.lang.UnsupportedOperationException();
    }

    @Override
    public int size() {
        return binaryObject.size();
    }

    @Override
    public boolean isEmpty() {
        return binaryObject.size()==0;
    }

    @Override
    public boolean containsKey(Object key) {
        return binaryObject.hasField(key.toString());
    }

    @Override
    public Object put(String key, Object value) {    	
        throw new java.lang.UnsupportedOperationException();
    }

    public void putIfNotNull(String key, Object value) {
        if (value != null) {
            put(key, value);
        }
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
    	throw new java.lang.UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
    	throw new java.lang.UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public BinaryDocument clone() {
        try {
			return new BinaryDocument((BinaryObjectImpl) binaryObject.clone());
		} catch (CloneNotSupportedException e) {
			return this;
		}
    }

    @Override
    public Set<String> keySet() {
    	if(binaryObject.size()==0) {
    		return Collections.EMPTY_SET;
    	}
        return (Set<String>)binaryObject.type().fieldNames();
    }

    @Override
    public Collection<Object> values() {
    	int len = this.binaryObject.size();
    	ArrayList<Object> enumValues = new ArrayList<>(len);
    	for(String field: this.binaryObject.type().fieldNames()) {
    		enumValues.add(this.binaryObject.field(field));
    	}
    	return enumValues;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
    	int len = this.binaryObject.size();    	
    	Set<Entry<String, Object>> enumValues = new HashSet<>(len*2);
    	for(String field: this.binaryObject.type().fieldNames()) {
    		enumValues.add(new IgniteBiTuple<>(field,this.binaryObject.field(field)));
    	}
    	return enumValues;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof BinaryDocument)) {
            return false;
        }        
        BinaryDocument other = (BinaryDocument) o;        
        return binaryObject.equals(other.binaryObject);
    }

    @Override
    public int hashCode() {
        return binaryObject.hashCode();
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean compactKey) {
        return toString(compactKey, "{", "}");
    }

    public String toString(boolean compactKey, String prefix, String suffix) {
        return entrySet().stream()
            .map(entry -> writeKey(entry.getKey(), compactKey) + " " + Json.toJsonValue(entry.getValue(), compactKey, prefix, suffix))
            .collect(Collectors.joining(", ", prefix, suffix));
    }

    private String writeKey(String key, boolean compact) {
        if (compact) {
            return (key) + ":";
        } else {
            return "\"" + (key) + "\" :";
        }
    }    

    public BinaryObjectImpl asBinaryObject() {
    	return this.binaryObject;
    }
}
