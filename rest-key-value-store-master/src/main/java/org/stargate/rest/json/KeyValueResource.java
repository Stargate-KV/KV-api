package org.stargate.rest.master;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.stargate.rest.json.KVDataType;
import org.stargate.rest.json.KVResponse;
import org.stargate.rest.json.KeyValPair;
import org.stargate.rest.json.KvstoreException;


// add authorization
@ApplicationScoped
@Path("/kvstore/v1")
public class KeyValueResource {
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
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createDB(String db_name_json, @HeaderParam(value = "X-Cassandra-Token") String token){
	  	int port = 8086;

		try {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases"))
					.header("X-Cassandra-Token", token)
					.POST(HttpRequest.BodyPublishers.ofString(db_name_json))
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
  public Response deleteDB(@PathParam("db_name") String db_name, @HeaderParam(value = "X-Cassandra-Token") throws KvstoreException {
	
    int port = 8086;
	try {
		HttpRequest request = HttpRequest.newBuilder()
				.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/" + db_name))
				.header("X-Cassandra-Token", token)
				.DELETE()
				.build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
  public Response createTable(@PathParam("db_name") String db_name, String table_name_json, @HeaderParam(value = "X-Cassandra-Token")
      throws KvstoreException, JsonProcessingException {
	  	int port = 8086;

		try {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/" + db_name +"/tables"))
					.header("X-Cassandra-Token", token)
					.POST(HttpRequest.BodyPublishers.ofString(db_name_json))
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
      @PathParam("db_name") String db_name, @PathParam("table_name") String table_name)
      throws KvstoreException {
  	int port = 8086;

	try {
		HttpRequest request = HttpRequest.newBuilder()
				.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/" + db_name + "/" + table_name))
				.header("X-Cassandra-Token", token)
				.DELETE()
				.build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
  public Response listDBs() throws KvstoreException {
		int port = 8086;

		try {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/"))
					.header("X-Cassandra-Token", token)
					.GET()
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
  public Response listTables(@PathParam("db_name") String db_name) throws KvstoreException {
		int port = 8086;

		try {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/" + db_name + "/tables"))
					.header("X-Cassandra-Token", token)
					.GET()
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
      String json_body)
      throws KvstoreException, JsonMappingException, JsonProcessingException {
		int port = 8086;

		try {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/" + db_name +"/"+table_name))
					.header("X-Cassandra-Token", token)
					.PUT(HttpRequest.BodyPublishers.ofString(json_body))
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
      KeyValPair kvPair)
      throws KvstoreException {
		int port = 8086;

		try {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/" + db_name + "/" + table_name))
					.header("X-Cassandra-Token", token)
					.GET()
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
      String json_body)
      throws KvstoreException, JsonMappingException, JsonProcessingException {
		int port = 8086;

		try {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/" + db_name +"/"+table_name))
					.header("X-Cassandra-Token", token)
					.PATCH(HttpRequest.BodyPublishers.ofString(json_body))
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
      KeyValPair kvPair)
      throws KvstoreException {
		int port = 8086;

		try {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases/" + db_name +"/"+table_name+"/key"))
					.header("X-Cassandra-Token", token)
					.DELETE()
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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

}
