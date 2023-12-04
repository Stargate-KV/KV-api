/**
 * KeyValueResource is a JAX-RS resource class providing RESTful endpoints for key-value store operations.
 */

package org.stargate.rest.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import org.json.JSONObject;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

// add authorization
@ApplicationScoped
@Path("/kvstore/v1")
public class KeyValueResource {
  ConsistentHashing consistentHashing;
  String[] servers;
  ObjectMapper objectMapper = new ObjectMapper();
  String resetCache;

  /**
   * Constructor for KeyValueResource initializes servers, consistent hashing, and cache reset configuration.
   */
  public KeyValueResource() {
    servers =
        new String[] {"http://host.docker.internal:8086", "http://host.docker.internal:8087", "http://host.docker.internal:8088"};

    consistentHashing = new ConsistentHashing(servers.length);
    for (String s : servers) {
      consistentHashing.addServer(s);
    }
    JSONObject json = new JSONObject();
    json.put("max_size", "-1");
    json.put("eviction_policy", "NOCHANGE");
    resetCache = json.toString();
  }

  // create and delete databases
  /**
   * @param db_name_json
   * @return
   * @throws KvstoreException
   * @throws JsonProcessingException
   */
  @POST
  @Path("databases")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createDB(
      String db_name_json, @HeaderParam(value = "X-Cassandra-Token") String token) {
	  try {
	    JsonNode jsonNode = objectMapper.readTree(db_name_json);
	    String db_name;
	    try {
	      db_name = jsonNode.get("db_name").asText();
	    } catch (Exception ex) {
	    	return Response.status(400).entity("Bad request, must provide a valid database name.").build();
	    }
	    String server = consistentHashing.getServer(db_name);


      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(server + "/kvstore/v1/databases"))
              .header("X-Cassandra-Token", token)
              .header("content-type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(db_name_json))
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * @param db_name
   * @return
   * @throws KvstoreException
   */
  @DELETE
  @Path("{db_name}")
  public Response deleteDB(
      @PathParam("db_name") String db_name,
      @HeaderParam(value = "X-Cassandra-Token") String token) {

    String server = consistentHashing.getServer(db_name);

    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(server + "/kvstore/v1/" + db_name))
              .header("X-Cassandra-Token", token)
              .DELETE()
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      for (String s: servers) {
    	  if (!server.equals(s)) {
    		  request =
    		          HttpRequest.newBuilder()
    		              .uri(new URI(server + "/kvstore/v1/resetcache"))
    		              .header("X-Cassandra-Token", token)
    		              .PUT(HttpRequest.BodyPublishers.ofString(resetCache))
    		              .build();
              HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    	  }
      }
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
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
  public Response createTable(
      @PathParam("db_name") String db_name,
      String table_name_json,
      @HeaderParam(value = "X-Cassandra-Token") String token) {

    String server = consistentHashing.getServer(db_name + "/" + table_name_json);
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(server + "/kvstore/v1/databases/" + db_name + "/tables"))
              .header("X-Cassandra-Token", token)
              .header("content-type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(table_name_json))
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * @param db_name
   * @param table_name
   * @return
   * @throws KvstoreException
   */
  @DELETE
  @Path("{db_name}/{table_name}")
  public Response deleteTable(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      @HeaderParam(value = "X-Cassandra-Token") String token) {

    String server = consistentHashing.getServer(db_name + "/" + table_name);

    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(server + "/kvstore/v1/" + db_name + "/" + table_name))
              .header("X-Cassandra-Token", token)
              .DELETE()
	              .build();
	      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
	   
      for (String s: servers) {
    	  if (!server.equals(s)) {
    		  request =
    		          HttpRequest.newBuilder()
    		              .uri(new URI(server + "/kvstore/v1/resetcache"))
    		              .header("X-Cassandra-Token", token)
    		              .PUT(HttpRequest.BodyPublishers.ofString(resetCache))
    		              .build();
              HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    	  }
      }
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * @return
   * @throws KvstoreException
   */
  @GET
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listDBs(@HeaderParam(value = "X-Cassandra-Token") String token) {
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(servers[0] + "/kvstore/v1/databases/"))
              .header("X-Cassandra-Token", token)
              .GET()
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * @param db_name
   * @return
   * @throws KvstoreException
   */
  @GET
  @Path("{db_name}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTables(
      @PathParam("db_name") String db_name,
      @HeaderParam(value = "X-Cassandra-Token") String token) {

    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(servers[1] + "/kvstore/v1/" + db_name + "/tables"))
              .header("X-Cassandra-Token", token)
              .GET()
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
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
  public Response putKeyVal(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      String json_body,
      @HeaderParam(value = "X-Cassandra-Token") String token)
      throws JsonMappingException, JsonProcessingException {

	try {
	    JsonNode jsonNode = objectMapper.readTree(json_body);
	    if (!jsonNode.has("key") || !jsonNode.has("value")) {
	    	return Response.status(400).entity("Bad request, must provide valid database, table name and key value pair.").build();
	    }
	    String key = jsonNode.get("key").asText();
	    String server = consistentHashing.getServer(key);
	  
	  HttpRequest request =
	      HttpRequest.newBuilder()
	          .uri(new URI(server + "/kvstore/v1/" + db_name + "/" + table_name))
	          .header("X-Cassandra-Token", token)
	          .header("content-type", "application/json")
	          .PUT(HttpRequest.BodyPublishers.ofString(json_body))
	          .build();
	  HttpResponse<String> response =
	      HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
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
  public Response getKeyVal(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      String json_body,
      @HeaderParam(value = "X-Cassandra-Token") String token)
{
	try {
	    JsonNode jsonNode = objectMapper.readTree(json_body);
	    if (!jsonNode.has("key")) {
	    	return Response.status(400).entity("Bad request, must provide a key.").build();
	    }
	    String key = jsonNode.get("key").asText();
	    String server = consistentHashing.getServer(key);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(server + "/kvstore/v1/" + db_name + "/" + table_name))
              .header("X-Cassandra-Token", token)
              .header("content-type", "application/json")
              .method("GET",HttpRequest.BodyPublishers.ofString(json_body))
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
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
  public Response updateKeyVal(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      String json_body,
      @HeaderParam(value = "X-Cassandra-Token") String token){
	try {
	    JsonNode jsonNode = objectMapper.readTree(json_body);
	    if (!jsonNode.has("key") || !jsonNode.has("value")) {
	    	return Response.status(400).entity("Bad request, must provide valid database, table name and key value pair.").build();
	    }
	    String key = jsonNode.get("key").asText();
	    String server = consistentHashing.getServer(key);

   
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(server + "/kvstore/v1/" + db_name + "/" + table_name))
              .header("X-Cassandra-Token", token)
              .header("content-type", "application/json")
              .method("PATCH",HttpRequest.BodyPublishers.ofString(json_body))
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
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
  public Response deleteKey(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      String json_body,
      @HeaderParam(value = "X-Cassandra-Token") String token)
 {
    try {
		JsonNode jsonNode = objectMapper.readTree(json_body);
	    if (!jsonNode.has("key")) {
	    	return Response.status(400).entity("Bad request, must provide a key.").build();
	    }
	    String key = jsonNode.get("key").asText();
	    String server = consistentHashing.getServer(key);

    
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(server + "/kvstore/v1/" + db_name + "/" + table_name + "/key"))
              .header("X-Cassandra-Token", token)
              .header("content-type", "application/json")
              .DELETE()
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      return Response.status(response.statusCode()).entity(response.body()).build();
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  @PUT
  @Path("resetcache")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response resetCache(String json_body, @HeaderParam(value = "X-Cassandra-Token") String token){
	  try {
		  HttpResponse<String> response;
		  for (String server: servers) {
		      HttpRequest request =
		          HttpRequest.newBuilder()
		              .uri(new URI(server + "/kvstore/v1/resetcache"))
		              .header("X-Cassandra-Token", token)
		              .header("content-type", "application/json")
		              .PUT(HttpRequest.BodyPublishers.ofString(json_body))
		              .build();
		       response =
		          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
		  }
	      return Response.ok().build();
	    } catch (URISyntaxException e1) {
	      // TODO Auto-generated catch block
	      e1.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    }
	    return null;
  }
  
  
  @GET
  @Path("getcachestatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCacheStatus(@HeaderParam(value = "X-Cassandra-Token") String token){
	  try {
		  List<String> res = new ArrayList<>();
		  for (String server: servers) {
		      HttpRequest request =
		          HttpRequest.newBuilder()
		              .uri(new URI(server + "/kvstore/v1/getcachestatus"))
		              .header("X-Cassandra-Token", token)
		              .GET()
		              .build();
		      HttpResponse<String> response =
		          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
		       res.add(response.body());
		  }
		  
	      return Response.status(Response.Status.OK).entity(String.join("|", res)).build();
	    } catch (URISyntaxException e1) {
	      // TODO Auto-generated catch block
	      e1.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    }
	    return null;
  }
}
