package org.stargate.rest.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import java.util.HashSet;
import java.util.Set;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.stargate.rest.json.KVCache;





// add authorization
@ApplicationScoped
@SecurityRequirement(name = "Token")
@Path("/kvstore/v1")
public class KeyValueResource {
  @Inject StargateBridgeClient bridge;
  @Inject KVCassandra kvcassandra;
  @Inject KVCache kvcache;
  ObjectMapper objectMapper = new ObjectMapper();
  
  public KeyValueResource() {}

  // create and delete databases
  /**
   * @param db_name_json
   * @return
   * @throws KvstoreException
   * @throws JsonProcessingException
   */
  @POST
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KVResponse createDB(String db_name_json) throws KvstoreException, JsonProcessingException {
    JsonNode jsonNode = objectMapper.readTree(db_name_json);
    String db_name;
    try {
      db_name = jsonNode.get("db_name").asText();
    } catch (Exception ex) {
      return new KVResponse(400, "Bad request, must provide a valid database name.");
    }
    KVResponse resonse = kvcassandra.createKeyspace(db_name);
    return resonse;
  }

  /**
   * @param db_name
   * @return
   * @throws KvstoreException
   */
  @DELETE
  @Path("{db_name}")
  public KVResponse deleteDB(@PathParam("db_name") String db_name) throws KvstoreException {
    if (db_name == null) {
      return new KVResponse(400, "Bad request, must provide a valid database name.");
    }
    KVResponse response = kvcassandra.deleteKeyspace(db_name);
    return response;
  }

  /**
   * @param db_name
   * @param table_name_json
   * @return
   * @throws KvstoreException
   * @throws JsonProcessingException
   */
  @POST
  @Path("databases/{db_name}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KVResponse createTable(@PathParam("db_name") String db_name, String table_name_json)
      throws KvstoreException, JsonProcessingException {
    JsonNode jsonNode = objectMapper.readTree(table_name_json);
    String table_name;
    try {
      table_name = jsonNode.get("table_name").asText();
    } catch (Exception ex) {
      return new KVResponse(400, "Bad request, must provide a valid table name.");
    }

    KVResponse response = kvcassandra.createTable(db_name, table_name);
    return response;
  }

  /**
   * @param db_name
   * @param table_name
   * @return
   * @throws KvstoreException
   */
  @DELETE
  @Path("{db_name}/{table_name}")
  public KVResponse deleteTable(
      @PathParam("db_name") String db_name, @PathParam("table_name") String table_name)
      throws KvstoreException {
    if (db_name == null || table_name == null) {
      return new KVResponse(400, "Bad request, must provide valid database name and table name.");
    }
    KVResponse response = kvcassandra.deleteTable(db_name, table_name);
    return response;
  }

  /**
   * @return
   * @throws KvstoreException
   */
  @GET
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  public KVResponse listDBs() throws KvstoreException {
    KVResponse response = kvcassandra.listKeyspaces();
    return response;
  }

  /**
   * @param db_name
   * @return
   * @throws KvstoreException
   */
  @GET
  @Path("{db_name}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  public KVResponse listTables(@PathParam("db_name") String db_name) throws KvstoreException {
    if (db_name == null) {
      return new KVResponse(400, "Bad request, must provide valid database name.");
    }
    KVResponse response = kvcassandra.listTables(db_name);
    return response;
  }
  
  private boolean checkType(JsonNode node, KVDataType type) {
	
  	switch(type) {
  	case INT:
  		return node.isIntegralNumber();
  	case DOUBLE:
  		return node.isDouble();
  	case TEXT:
  		return node.isTextual();
  	default:
  		return false;
  	}

  }
  private KVDataType getTypeForRequest(JsonNode jsonNode, JsonNode value) throws KvstoreException {
	  KVDataType type = null;
	  if (jsonNode.has("type")) {
	    	String type_str = jsonNode.get("type").asText().toLowerCase().replaceAll("\\s+","");
	    	try {
	    		type = KVDataType.get(type_str);
	    	}catch(IllegalArgumentException ex) {
	    		 throw new KvstoreException(
		          	          400, "Bad request, must provide valid type, value name.");
			}
	    	

	    	switch(type) {
	    	case INT:
	    	case DOUBLE:
	    	case TEXT:
	    		if (!checkType(value, type)) {
	   	    		 throw new KvstoreException(
	   	          	          400, "Bad request, must provide valid type, value name.");
	   			}
	   			break;
	    	default:
	    		if (!value.isArray()) {
	   	    		 throw new KvstoreException(
	   	          	          400, "Bad request, must provide valid type, value name.");
	   			}
	    		for (JsonNode node: (ArrayNode)value) {
	    			if (!checkType(node, KVCassandra.DATAMAP.get(type))){
	    				 throw new KvstoreException(
	      	          	          400, "Bad request, must provide valid type, value name.");
	    			}
	    		}
	   			break;
	    	}
	    	
	    }else{
	    	JsonNodeType nodeType = value.getNodeType();
	    	switch(nodeType) {
	    	case STRING:
	    		type = KVDataType.TEXT;
	    		break;
	    	case NUMBER:
	    		if (value.isIntegralNumber()) {
	    			type = KVDataType.INT;
	    		}else if(value.isDouble()) {
	    			type = KVDataType.DOUBLE;
	    		}
	    		break;
	    	case ARRAY:
	    		Set<JsonNodeType> datatype_set = new HashSet<>();
	    		boolean is_int = true;
	    		for (JsonNode node: (ArrayNode)value) {
	    	
	    			switch(node.getNodeType()) {
	    			case NUMBER:
	    				if (node.isFloatingPointNumber()) {
	    	    			is_int = false;
	    	    		}
	    		   	case STRING:
	    	    		datatype_set.add(node.getNodeType());
	    	    		break;   
	    	    	default:
	    	    		throw new KvstoreException(
	    	   					400, "value format not supported.");
	    			}
	    		}
	    		if (datatype_set.size() == 1) {
	    			JsonNodeType array_node_type = datatype_set.iterator().next();
	    			switch(array_node_type) {
	    				case STRING:
	    					type = KVDataType.LISTTEXT;
	    					break;
	    				case NUMBER:
	    					if (is_int) {
	    						type = KVDataType.LISTINT;
	    					}else{
	    						type = KVDataType.LISTDOUBLE;
	    					}
	    			}
	    		}else {
	    			type = KVDataType.LISTTEXT;
	    		}
	    			
	    		break;
	    	default:
	    		throw new KvstoreException(
	   					400, "value format not supported.");
	    	}
	      	
	    }
	  return type;
  }
  /**
   * @param db_name
   * @param table_name
   * @param kvPair
   * @return
   * @throws KvstoreException
 * @throws JsonProcessingException 
 * @throws JsonMappingException 
   */
  @PUT
  @Path("{db_name}/{table_name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KVResponse putKeyVal(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      String json_body)
      throws KvstoreException, JsonMappingException, JsonProcessingException {
    if (db_name == null
        || table_name == null
        || json_body == null
    ) {
      throw new KvstoreException(
          400, "Bad request, must provide valid database, table name and key value pair.");
    }
    JsonNode jsonNode = objectMapper.readTree(json_body);
    if (!jsonNode.has("key") || !jsonNode.has("value")  ) {
    	 throw new KvstoreException(
    	          400, "Bad request, must provide valid database, table name and key value pair.");
    }
    String key = jsonNode.get("key").asText();
    JsonNode value = jsonNode.get("value");

   
    KVDataType type = getTypeForRequest(jsonNode, value);
    // Not sure whether the key already exists or not, set dirty to true
    KVResponse cacheResponse = kvcache.createOrUpdate(key, value, db_name, table_name, type, true);
    return cacheResponse;
    // KVResponse response = kvcassandra.putKeyVal(db_name, table_name, key, value, type);
    // return response;
  }

  /**
   * @param db_name
   * @param table_name
   * @param kvPair
   * @return
   * @throws KvstoreException
   */
  @GET
  @Path("{db_name}/{table_name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KVResponse getKeyVal(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      KeyValPair kvPair)
      throws KvstoreException {
    if (db_name == null || table_name == null || kvPair == null || kvPair.key == null) {
      return new KVResponse(
          400, "Bad request, must provide valid database, table name and key value pair.");
    }
    if (kvcache.read(kvPair.key, db_name, table_name) == null) {
      // Does not exists in cache, read from cassandra first
      KVResponse response = kvcassandra.getVal(db_name, table_name, kvPair.key);
      JsonNode value = response.body.getJsonBody();
      // Not dirty because we did not modify it
      kvcache.createOrUpdate(kvPair.key, value, db_name, table_name, response.body.type, false);
      return response;
    } else {
      return new KVResponse(200, "The key '" + kvPair.key + "' has a value of" + kvcache.read(kvPair.key, db_name, table_name).getValue());
    }
    // KVResponse response = kvcassandra.getVal(db_name, table_name, kvPair.key);
    // return response;
  }

  /**
   * @param db_name
   * @param table_name
   * @param kvPair
   * @return
   * @throws KvstoreException
 * @throws JsonProcessingException 
 * @throws JsonMappingException 
   */
  @PATCH
  @Path("{db_name}/{table_name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KVResponse updateKeyVal(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      String json_body)
      throws KvstoreException, JsonMappingException, JsonProcessingException {
	  if (db_name == null
		        || table_name == null
		        || json_body == null
	    ) {
	      throw new KvstoreException(
	          400, "Bad request, must provide valid database, table name and key value pair.");
	    }
	    JsonNode jsonNode = objectMapper.readTree(json_body);
	    if (!jsonNode.has("key") || !jsonNode.has("value")  ) {
	    	 throw new KvstoreException(
	    	          400, "Bad request, must provide valid database, table name and key value pair.");
	    }
	    String key = jsonNode.get("key").asText();
	    JsonNode value = jsonNode.get("value");

	   
	    KVDataType type = getTypeForRequest(jsonNode, value);

    // Not sure whether the key-value pair exists in cassandra or not, so set it to dirty
    KVResponse cacheResponse = kvcache.createOrUpdate(key, value, db_name, table_name, type, true);
    return cacheResponse;
    // KVResponse response = kvcassandra.updateVal(db_name, table_name, key, value, type);
    // return response;
  }

  /**
   * @param db_name
   * @param table_name
   * @param kvPair
   * @return
   * @throws KvstoreException
   */
  @DELETE
  @Path("{db_name}/{table_name}/key")
  @Consumes(MediaType.APPLICATION_JSON)
  public KVResponse deleteKey(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      KeyValPair kvPair)
      throws KvstoreException {
    if (db_name == null
        || table_name == null
        || kvPair == null
        || kvPair.key == null) {
      return new KVResponse(
          400, "Bad request, must provide valid database, table name and key value pair.");
    }
    if (kvcache.read(kvPair.key, db_name, table_name) != null) {
      // delete entry in cache
      kvcache.delete(kvPair.key, db_name, table_name);
    }
    KVResponse response = kvcassandra.deleteKey(db_name, table_name, kvPair.key);
    return response;
  }
}
