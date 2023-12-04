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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter; 
import java.io.File;
 
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
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

    // TODO: store the hashing between key value and servers in persistent storage
    consistentHashing = new ConsistentHashing(servers.length);
    for (String s : servers) {
      consistentHashing.addServer(s);
    }
    JSONObject json = new JSONObject();
    json.put("max_size", "-1");
    json.put("eviction_policy", "NOCHANGE");
    resetCache = json.toString();
  }

  /**
   * create db
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
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * delete db
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
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * create a table 
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
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * delete a table
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
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * get all the current databases 
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
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * get all the current tables 
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
      
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * put key value pair into table
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
      
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * get the current value of the key 
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
      
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * update a key value pair in the table
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
      
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * delete a key from the table
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
      
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  /**
   * reset the cache of slave servers
   */
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
	      
	      e1.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    }
	    return null;
  }
  
  /**
   * get the current cache status of all the slave servers
   */
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
	      
	      e1.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    }
	    return null;
  }
  
  // get the metrics and store them in a directory from all the slave servers
  @GET
  @Path("metrics")
  public Response getMetrics(
	      String json_body, @HeaderParam(value = "X-Cassandra-Token") String token)
	 {
	    try {
	    	String dir = "metrics";
			  JsonNode jsonNode = objectMapper.readTree(json_body);
		    if (jsonNode.has("dir")) {
		    	dir = jsonNode.get("dir").asText();
		    }

		    File directory = new File(dir);

		    if (! directory.exists()){
           Files.createDirectory(directory.toPath());
		    }
		    for(int i = 0; i < servers.length; i++) {
		    	  String server = servers[i];
			      HttpRequest request =
				          HttpRequest.newBuilder()
				              .uri(new URI(server + "/q/metrics/"))
				              .header("X-Cassandra-Token", token)
				              .GET()
				              .build();
				      HttpResponse<String> response =
				          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
	
				      BufferedWriter writer = new BufferedWriter(new FileWriter(dir + "/" + "server" + i + ".txt"));
				      writer.write(response.body());
				      writer.close();
		    }

	      return Response.status(Response.Status.OK).build();
	    } catch (URISyntaxException e1) {
	      
	      e1.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    }
	    return null;
	  }
  
  
}
