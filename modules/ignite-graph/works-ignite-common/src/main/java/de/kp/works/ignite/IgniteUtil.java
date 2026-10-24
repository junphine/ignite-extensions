package de.kp.works.ignite;


import org.apache.ignite.configuration.*;
import org.apache.ignite.binary.*;
import org.apache.ignite.*;
import org.apache.ignite.affinity.KeyPrefixAffinityKeyMapper;

import de.kp.works.ignite.graph.*;
import org.apache.ignite.cache.*;


import java.util.*;

public final class IgniteUtil{
	public static IgniteUtil MODULE$ = new IgniteUtil();
	
    
    public void createCacheIfNotExists(final Ignite ignite, final String table, final CacheConfiguration<String, BinaryObject> cfg) {
        final boolean exists = ignite.cacheNames().contains(table);
        if (!exists) {
            ignite.createCache(cfg);
        }
    }
    
    public IgniteCache<String, BinaryObject> getOrCreateCache(final Ignite ignite, final String table, final String namespace) {
        final boolean exists = ignite.cacheNames().contains(table);
        return (exists ? ignite.cache(table) : this.createCache(ignite, table, namespace));
    }
    
    public IgniteCache<String, BinaryObject> createCache(final Ignite ignite, final String table, final String namespace) {
        return this.createCache(ignite, table, namespace, CacheMode.PARTITIONED);
    }
    
    public IgniteCache<String, BinaryObject> createCache(final Ignite ignite, final String name, final String namespace, final CacheMode cacheMode) {
        
        ElementType elementType = null;
        /*
         * Retrieve element type from provided
         * cache (table) name
         */
        if (name.equals(namespace + "_" + IgniteConstants.EDGES)) {
            elementType = ElementType.EDGE;
        }
        else if (name.equals(namespace + "_" + IgniteConstants.VERTICES)) {
            elementType = ElementType.VERTEX;
        }
        else if (name.startsWith(namespace + "_")) {
            elementType = ElementType.DOCUMENT;
        }
        else if (name.startsWith(namespace + ".")) {
            elementType = ElementType.DOCUMENT;
        }
        else {
            elementType = ElementType.UNDEFINED;
            throw new RuntimeException("table name not support!"+name);
        }
        final ElementType tableType = elementType;
        final CacheConfiguration<String, BinaryObject> cfg = this.createCacheCfg(name, tableType, cacheMode);
        return ignite.createCache(cfg);
    }
    
    public CacheConfiguration<String, BinaryObject> createCacheCfg(final String table, final ElementType tableType, final CacheMode cacheMode) {
        final QueryEntity qe = this.buildQueryEntity(table, tableType);
        final ArrayList<QueryEntity> qes = new ArrayList<>();
        qes.add(qe);
        final CacheConfiguration<String, BinaryObject> cfg = new CacheConfiguration<>();
        cfg.setName(table);
        cfg.setStoreKeepBinary(false);
        
        cfg.setCacheMode(cacheMode);
        cfg.setQueryEntities(qes);
        // add@byron
        cfg.setAffinityMapper(new KeyPrefixAffinityKeyMapper());
        // end@
        return cfg;
    }
    
    public QueryEntity buildQueryEntity(final String table, final ElementType elementType) {
        final QueryEntity qe = new QueryEntity();
        qe.setKeyType("java.lang.String");
        qe.setValueType(table);
        
        if (elementType == ElementType.EDGE) {
        	
        	qe.setFields(this.buildEdgeFields());
            qe.setIndexes(this.buildEdgeIndexs());
            return qe;
        }
        else if(elementType == ElementType.VERTEX) {
        	
        	qe.setFields(this.buildVertexFields());
            qe.setIndexes(this.buildVertexIndexs());
            return qe;
        } 
        if (elementType == null || !elementType.equals(ElementType.DOCUMENT)) {
            throw new RuntimeException(new StringBuilder().append("Table '").append(table).append("' is not supported.").toString());
        }
        qe.setFields(this.buildDocumentFields(qe));
        
        if(!table.endsWith("_index")) {
        	qe.setIndexes(this.buildDocumentIndexs());
        }
        return qe;
    }
    
    public LinkedHashMap<String, String> buildEdgeFields() {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put(IgniteConstants.EDGE_ID_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.PROPERTY_WEIGHT_COL_NAME, "java.lang.Float");
        fields.put(IgniteConstants.LABEL_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.TO_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.TO_TYPE_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.FROM_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.FROM_TYPE_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.CREATED_AT_COL_NAME, "java.lang.Long");
        fields.put(IgniteConstants.UPDATED_AT_COL_NAME, "java.lang.Long");
        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.PROPERTY_TYPE_COL_NAME, "java.lang.Short");
        fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.PROPERTY_NUMBER_COL_NAME, "java.math.BigDecimal");
        return fields;
    }
    
    public List<QueryIndex> buildEdgeIndexs() {
        final List<QueryIndex> indexes = new ArrayList<>();
        indexes.add(new QueryIndex(IgniteConstants.EDGE_ID_COL_NAME));
        indexes.add(new QueryIndex(IgniteConstants.FROM_COL_NAME));
        indexes.add(new QueryIndex(IgniteConstants.TO_COL_NAME));
        indexes.add(new QueryIndex(IgniteConstants.LABEL_COL_NAME));
        return indexes;
    }
    
    public LinkedHashMap<String, String> buildVertexFields() {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put(IgniteConstants.VERTEX_ID_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.ID_TYPE_COL_NAME, "java.lang.Short");
        fields.put(IgniteConstants.LABEL_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.CREATED_AT_COL_NAME, "java.lang.Long");
        fields.put(IgniteConstants.UPDATED_AT_COL_NAME, "java.lang.Long");
        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.PROPERTY_TYPE_COL_NAME, "java.lang.Short");
        fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.PROPERTY_NUMBER_COL_NAME, "java.math.BigDecimal");
        return fields;
    }
    
    public List<QueryIndex> buildVertexIndexs() {
        final List<QueryIndex> indexes = new ArrayList<>();
        indexes.add(new QueryIndex(IgniteConstants.VERTEX_ID_COL_NAME));
        indexes.add(new QueryIndex(IgniteConstants.LABEL_COL_NAME));
        indexes.add(new QueryIndex(Arrays.asList(IgniteConstants.LABEL_COL_NAME,IgniteConstants.PROPERTY_KEY_COL_NAME),QueryIndexType.SORTED));
        indexes.add(new QueryIndex(IgniteConstants.PROPERTY_NUMBER_COL_NAME,QueryIndexType.SORTED,false));
        return indexes;
    }
    
    public LinkedHashMap<String, String> buildDocumentFields(QueryEntity qe) {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.String");
        fields.put("type", "java.lang.String");        
        fields.put("created_at", "java.lang.Long");
        fields.put("updated_at", "java.lang.Long");
        fields.put("name", "java.lang.String");
        fields.put("title", "java.lang.String");
        fields.put("author", "java.lang.String");
        
        qe.setKeyFieldName("id");
        return fields;
    }
    
    public List<QueryIndex> buildDocumentIndexs() {
        final List<QueryIndex> indexes = new ArrayList<>();
        indexes.add(new QueryIndex("name",QueryIndexType.SORTED));
        indexes.add(new QueryIndex("updated_at",QueryIndexType.SORTED,false));
        return indexes;
    }
}

