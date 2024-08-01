package org.elasticsearch.relay.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.rest.handlers.query.CacheQueryFieldsMetaResult;
import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.model.ESQuery;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;


public class ESUtil {
	
	private static ObjectNode getFilterObject(ESQuery query) {
		// check if there is a query
		ObjectNode jsonQuery = query.getQuery();
		if (jsonQuery == null) {
			jsonQuery = new ObjectNode(ESRelay.jsonNodeFactory);
			query.setQuery(jsonQuery);
		}

		// check if there is a query sub-object
		ObjectNode queryObj = (ObjectNode)jsonQuery.get(ESConstants.Q_QUERY);
		if (queryObj == null) {
			queryObj = new ObjectNode(ESRelay.jsonNodeFactory);
			jsonQuery.set(ESConstants.Q_QUERY, queryObj);
		}

		// check if there is a filtered sub-object
		ObjectNode filteredObj = (ObjectNode)queryObj.get(ESConstants.Q_FILTERED);
		if (filteredObj == null) {
			filteredObj = new ObjectNode(ESRelay.jsonNodeFactory);
			queryObj.set(ESConstants.Q_FILTERED, filteredObj);
		}

		// check if there is a filter sub-object
		ObjectNode filterObj = (ObjectNode)filteredObj.get(ESConstants.Q_FILTER);
		if (filterObj == null) {
			filterObj = new ObjectNode(ESRelay.jsonNodeFactory);
			filteredObj.set(ESConstants.Q_FILTER, filterObj);
		}

		return filterObj;
	}

	public static ArrayNode getOrCreateFilterArray(ESQuery query) {
		ObjectNode filterObj = getFilterObject(query);

		// actual array of filters
		// check if there is a logical 'and' array
		ArrayNode andArray = filterObj.withArray("/"+ESConstants.Q_AND);
		if (andArray == null) {
			andArray = new ArrayNode(ESRelay.jsonNodeFactory);
			filterObj.set(ESConstants.Q_AND, andArray);
		}

		return andArray;
	}

	public static void replaceFilterArray(ESQuery query, ArrayNode andArray) throws Exception {
		ObjectNode filterObj = getFilterObject(query);

		// remove existing array
		filterObj.remove(ESConstants.Q_AND);

		// replace with given or new one
		if (andArray == null) {
			andArray = new ArrayNode(ESRelay.jsonNodeFactory);
		}
		filterObj.set(ESConstants.Q_AND, andArray);
	}
	
	
	 /**
     * @param meta Internal query field metadata.
     * @return Rest query field metadata.
     */
    public static Collection<CacheQueryFieldsMetaResult> convertMetadata(Collection<?> meta) {
        List<CacheQueryFieldsMetaResult> res = new ArrayList<>();

        if (meta != null) {
            for (Object info : meta) {
            	if(info instanceof GridQueryFieldMetadata) {
            		res.add(new CacheQueryFieldsMetaResult((GridQueryFieldMetadata)info));
            	}
            	else if(info instanceof String) {
            		CacheQueryFieldsMetaResult metaData = new CacheQueryFieldsMetaResult();
            		metaData.setFieldName(info.toString());
            		res.add(metaData);
            	}
            }
                
        }

        return res;
    }
	
}
