package org.elasticsearch.relay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.ignite.Ignition;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.jackson.IgniteObjectMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.elasticsearch.relay.handler.ESQueryClientIgniteHandler;
import org.elasticsearch.relay.handler.ESQueryHandler;
import org.elasticsearch.relay.handler.ESQueryKernelIgniteHandler;
import org.elasticsearch.relay.model.ESDelete;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.model.ESViewQuery;
import org.elasticsearch.relay.util.ESConstants;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.webmvc.Vertxlet;
import io.vertx.webmvc.annotation.VertxletMapping;



/**
 * Elasticsearch Relay main servlet taking in all GET and POST requests and
 * their bodies and returning the response created by the ESQueryHandler.
 */
@Service
@VertxletMapping(url="/es-relay/*")
public class ESRelayVertxlet extends Vertxlet {
	private static final String CONTENT_TYPE = "application/json; charset=UTF-8";
	
	public static ScheduledExecutorService scheduleExecutorService = Executors.newSingleThreadScheduledExecutor();
	   
	public static IgniteObjectMapper objectMapper = null;
	
	public static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
	

	private final ESRelayConfig fConfig;

	private final Logger fLogger;

	private ESQueryHandler fHandler;
	
	
	public Map<String, ESViewQuery> allViews = null;	
	

	public ESRelayVertxlet() {
		fConfig = new ESRelayConfig();
		fLogger = Logger.getLogger(this.getClass().getName());
	}

	@Override
	public void init() {
		// initialize query handler
		try {
			ApplicationContext context = ESRelay.applicationContext();
			
			allViews = context.getBeansOfType(ESViewQuery.class);			
			
			if("elasticsearch".equalsIgnoreCase(fConfig.getClusterBackend())){				
				objectMapper = new IgniteObjectMapper();
				fHandler = new ESQueryHandler(fConfig);
			}
			else if("igniteClient".equalsIgnoreCase(fConfig.getClusterBackend())){				
				fHandler = new ESQueryClientIgniteHandler(fConfig);				
				objectMapper = new IgniteObjectMapper();
			}
			else{				
				GridKernalContext ctx = ((IgniteEx)IgnitionEx.allGridsx().get(0)).context();
				if(ctx==null){
					fHandler = new ESQueryClientIgniteHandler(fConfig);
				}
				else{						
					fHandler = new ESQueryKernelIgniteHandler(fConfig,ctx);
				}
				objectMapper = new IgniteObjectMapper(ctx);
			}
			fLogger.info("init hander:" + fHandler.getClass());
			
		} catch (Exception e) {
			fLogger.severe("init ESRelay:" +e.getMessage());			
		}
	}
	
	
	/**
     * Parses HTTP parameters in an appropriate format and return back map of values to predefined list of names.
     *
     * @param req Request.
     * @return Map of parsed parameters.
     */
    private Map<String, String> getParams(HttpServerRequest req) {
        MultiMap params = req.params();

        if (F.isEmpty(params))
            return Collections.emptyMap();

        Map<String, String> map = U.newHashMap(params.size());

        for (Map.Entry<String, String> entry : req.params()) {
        	String lastValue = map.get(entry.getKey());
        	if(lastValue!=null) {
        		lastValue = lastValue+','+entry.getValue();
        	}
        	else {
        		lastValue = entry.getValue();
        	}
            map.put(entry.getKey(), lastValue);
        }

        return map;
    }

	private ObjectNode getJSONBody(String cmd, RoutingContext rc) {
		
		ObjectNode jsonRequest = new ObjectNode(jsonNodeFactory);

		try {
			
			String body = rc.body().asString("UTF-8");

			if(cmd.equals(ESConstants.BULK_FRAGMENT)){
				ArrayNode list = new ArrayNode(jsonNodeFactory);
				String[] lines = body.split("\n");
				for(String line: lines) {
					if(!line.isBlank() && line.length()>1) {
						list.add(objectMapper.readTree(line));
					}	
				}
				jsonRequest.set(ESConstants.BULK_FRAGMENT, list);
			}
			else {
			
				JsonNode jsonData = objectMapper.readTree(body);
				if(jsonData.isObject()) {
					jsonRequest = (ObjectNode)jsonData;
				}
				else {
					fLogger.log(Level.SEVERE, "request data is not json object");
				}
			}
			
		} catch (IOException e) {
			return null;
		}
		catch (Exception e) {
			// TODO: ?
			fLogger.log(Level.SEVERE, "failed to read request body", e);
		}

		return jsonRequest;
	}

	/**	 
	 * @param request
	 * @return
	 */
	private String[] getFixedPath(HttpServerRequest request) {
		// arrange path elements in a predictable manner
		String pathString = request.path().replaceFirst("/es-relay/", "");
		pathString = StringUtils.trimLeadingCharacter(pathString, '/');
		pathString = StringUtils.trimTrailingCharacter(pathString, '/');
		
		String[] path = pathString.split("/");
		if(path.length==1) {
			String[] fixedPath = new String[2];
			if(path[0].isBlank()) {  // home endpoint
				fixedPath[0] = path[0];
				fixedPath[1] = ESConstants.ALL_FRAGMENT;
			}
			else if(path[0].charAt(0)=='_') {
				fixedPath[0] = "";
				fixedPath[1] = path[0];
			}
			else {
				fixedPath[0] = path[0];
				fixedPath[1] = ESConstants.INDICES_FRAGMENT;
			}
			return fixedPath;
		}
		return path;
	}

	/**
	 * query cmd and ignite rest api
	 * {index}/_cmd or {index}/_doc/{id}
	 */
	@Override
	public void handle(RoutingContext rc) {
	
		HttpServerRequest request = rc.request(); 
		HttpServerResponse response = rc.response();
		String method = request.method().name();
		// get authenticated user
		User userObj = rc.user();
		String user = userObj!=null? userObj.subject():null;
		// extract and forward request path
		String[] path = getFixedPath(request);
		String action = path[1];
		// properly extract query parameters
		
		
		Map<String, String> parameters = getParams(request);
		
		try {
			
			
			String result = null;

			//use ignite rest backends  /_cmd/put?cacheName=test&key=k1
			if("_cmd".equalsIgnoreCase(path[0])){
				
				parameters.put("cmd", path[1]);				

				// read request body
				
				ObjectNode jsonRequest = getJSONBody(path[0],rc);
					
				ESUpdate query = new ESUpdate(new String[] {"","_cmd"}, parameters, jsonRequest);
				
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
				
			}
			// handle views
			else if("_views".equalsIgnoreCase(path[0])) { // views query /_views/{schema}/{name}
				String viewName = String.join(".", Arrays.copyOfRange(path,1,2));
				ESViewQuery viewQuery = allViews.get(viewName);
				if(viewQuery==null) { // path[1] is schema not viewName,  q is SQL 	
					if(path.length>=3) {
						viewQuery = new ESViewQuery(path[1],path[2],request.getParam("q"));
					}
					else {
						viewQuery = new ESViewQuery(path[1],request.getParam("q"));
					}
				}
				else {
					viewQuery = new ESViewQuery(viewQuery);
					viewQuery.setName(viewName);
				}
				viewQuery.setQuery(null);
				viewQuery.setParams(parameters);
				if(!method.equals("GET")){
					ObjectNode jsonRequest = getJSONBody(action,rc);
					viewQuery.setQuery(jsonRequest);
				}
				// process request, forward to ES instances
				result = fHandler.handleRequest(viewQuery, user);	
				
			}
			else if(method.equals("GET") 
					|| action.equalsIgnoreCase(ESConstants.SEARCH_FRAGMENT) 
					|| action.equalsIgnoreCase(ESConstants.ALL_FRAGMENT)
					|| action.equalsIgnoreCase(ESConstants.GET_FRAGMENT)){
				
				
				if(!method.equals("GET")){
					ObjectNode jsonRequest = getJSONBody(action,rc);
					ESQuery query = new ESQuery(path, parameters, jsonRequest);
					
					// process request, forward to ES instances
					result = fHandler.handleRequest(query, user);	
				}
				else {
					// handle search
					ESQuery query = new ESQuery(path, parameters);
					
					ObjectNode jsonRequest = objectMapper.convertValue(parameters, ObjectNode.class);
					query.setQuery(jsonRequest);
					// process request, forward to ES instances
					result = fHandler.handleRequest(query, user);	
				}
				
			}
			else if(method.equals("POST")){
				
				ObjectNode jsonRequest = getJSONBody(action,rc);
				ESUpdate query = new ESUpdate(path, parameters, jsonRequest);
				query.setOp(ESConstants.INSERT_FRAGMENT);
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
			
			}
			else if(method.equals("PUT")){
				
				ObjectNode jsonRequest = getJSONBody(action,rc);
				ESUpdate query = new ESUpdate(path, parameters, jsonRequest);
				query.setOp(ESConstants.UPDATE_FRAGMENT);
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
			
			}
			else if(method.equals("DELETE")){
				
				ESDelete query = new ESDelete(path, parameters);
				
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);	
			
			}
			else if(method.equals("HEAD")){
				// handle exists
				ESQuery query = new ESQuery(path, parameters);				
				// process request, forward to ES instances
				result = fHandler.handleRequest(query, user);
				if(result.isBlank() || result.equals("{}") || result.equals("[]")) {
					throw new NoSuchElementException(Arrays.toString(path));
				}
			}
			else{				
				throw new UnsupportedOperationException();
			}
			// return result			
			response.putHeader("Content-Type", CONTENT_TYPE);
			response.end(result);
			
		} catch (NoSuchElementException e) {
			response.setStatusCode(404);
			response.reset();

			ObjectNode jsonError = new ObjectNode(jsonNodeFactory);
			jsonError.put(ESConstants.R_ERROR, e.getMessage());
			jsonError.put(ESConstants.R_STATUS, 404);
			
			response.end(jsonError.toPrettyString());
			
			fLogger.log(Level.SEVERE, "Error during error JSON generation", e);
			
		} catch (Exception e) {
			
			response.reset();

			ObjectNode jsonError = new ObjectNode(jsonNodeFactory);
			jsonError.put(ESConstants.R_ERROR, e.getMessage());
			jsonError.put(ESConstants.R_STATUS, 500);
			if(e.getClass().getSimpleName().indexOf("NotFound")>=0) {
				jsonError.put(ESConstants.R_STATUS, 404);
				response.setStatusCode(404);
			}
			if(e.getClass().getSimpleName().indexOf("Access")>=0) {
				jsonError.put(ESConstants.R_STATUS, 402);
				response.setStatusCode(402);
			}
			else {
				response.setStatusCode(500);
			}
			
			response.end(jsonError.toPrettyString());
			
			fLogger.log(Level.SEVERE, "Error during error JSON generation", e);
		}

	}
	

	@Override
	public void destroy() {
		// destroy query handler and its threads
		fHandler.destroy();
		fHandler = null;
	}
}