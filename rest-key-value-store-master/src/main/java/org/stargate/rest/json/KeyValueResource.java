package org.stargate.rest.master;


import com.fasterxml.jackson.core.JsonProcessingException;

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
      			System.out.println("http://10.0.2.2:" + port + "/kvstore/v1/databases");
			HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://172.18.0.1:" + port + "/kvstore/v1/databases"))
					.header("X-Cassandra-Token", token)
					.POST(HttpRequest.BodyPublishers.ofString(db_name_json))
					.build();
			HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      System.out.println("got a response with status code: "+response.statusCode() + " response body: " + response.body());
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
