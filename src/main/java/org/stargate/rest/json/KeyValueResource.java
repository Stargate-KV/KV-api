package org.stargate.rest.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;

// add authorization
@ApplicationScoped
@SecurityRequirement(name = "Token")
@Path("/kvstore/v1")
public class KeyValueResource {
  @Inject StargateBridgeClient bridge;
  @Inject KVCassandra kvcassandra;
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
  public Response createDB(String db_name_json) throws KvstoreException, JsonProcessingException {
    JsonNode jsonNode = objectMapper.readTree(db_name_json);
    String db_name;
    try {
      db_name = jsonNode.get("db_name").asText();
    } catch (Exception ex) {
      KVResponse kvResponse =
          new KVResponse(400, "Bad request, must provide a valid database name.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }
    KVResponse response = kvcassandra.createKeyspace(db_name);
    return Response.status(response.status_code).entity(response).build();
  }

  /**
   * @param db_name
   * @return
   * @throws KvstoreException
   */
  @DELETE
  @Path("{db_name}")
  public Response deleteDB(@PathParam("db_name") String db_name) throws KvstoreException {
    if (db_name == null) {
      KVResponse kvResponse =
          new KVResponse(400, "Bad request, must provide a valid database name.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }
    KVResponse response = kvcassandra.deleteKeyspace(db_name);
    return Response.status(response.status_code).entity(response).build();
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
  public Response createTable(@PathParam("db_name") String db_name, String table_name_json)
      throws KvstoreException, JsonProcessingException {
    JsonNode jsonNode = objectMapper.readTree(table_name_json);
    String table_name;
    try {
      table_name = jsonNode.get("table_name").asText();
    } catch (Exception ex) {
      KVResponse kvResponse = new KVResponse(400, "Bad request, must provide a valid table name.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }

    KVResponse response = kvcassandra.createTable(db_name, table_name);
    return Response.status(response.status_code).entity(response).build();
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
    if (db_name == null || table_name == null) {
      KVResponse kvResponse =
          new KVResponse(400, "Bad request, must provide valid database name and table name.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }
    KVResponse response = kvcassandra.deleteTable(db_name, table_name);
    return Response.status(response.status_code).entity(response).build();
  }

  /**
   * @return
   * @throws KvstoreException
   */
  @GET
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listDBs() throws KvstoreException {
    KVResponse response = kvcassandra.listKeyspaces();
    return Response.status(response.status_code).entity(response).build();
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
    if (db_name == null) {
      KVResponse kvResponse = new KVResponse(400, "Bad request, must provide valid database name.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }
    KVResponse response = kvcassandra.listTables(db_name);
    return Response.status(response.status_code).entity(response).build();
  }

  /**
   * @param db_name
   * @param table_name
   * @param kvPair
   * @return
   * @throws KvstoreException
   */
  @PUT
  @Path("{db_name}/{table_name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response putKeyVal(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      KeyValPair kvPair)
      throws KvstoreException {
    if (db_name == null
        || table_name == null
        || kvPair == null
        || kvPair.key == null
        || kvPair.value == null) {
      KVResponse kvResponse =
          new KVResponse(
              400, "Bad request, must provide valid database, table name and key value pair.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }
    KVResponse response = kvcassandra.putKeyVal(db_name, table_name, kvPair.key, kvPair.value);
    return Response.status(response.status_code).entity(response).build();
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
    if (db_name == null || table_name == null || kvPair == null || kvPair.key == null) {
      KVResponse kvResponse =
          new KVResponse(
              400, "Bad request, must provide valid database, table name and key value pair.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }
    KVResponse response = kvcassandra.getVal(db_name, table_name, kvPair.key);
    return Response.status(response.status_code).entity(response).build();
  }

  /**
   * @param db_name
   * @param table_name
   * @param kvPair
   * @return
   * @throws KvstoreException
   */
  @PATCH
  @Path("{db_name}/{table_name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response updateKeyVal(
      @PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name,
      KeyValPair kvPair)
      throws KvstoreException {
    if (db_name == null
        || table_name == null
        || kvPair == null
        || kvPair.key == null
        || kvPair.value == null) {
      KVResponse kvResponse =
          new KVResponse(
              400, "Bad request, must provide valid database, table name and key value pair.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }
    // m_kvservice.putKeyVal(db_id, kvPair.key, kvPair.value, true);
    KVResponse response = kvcassandra.updateVal(db_name, table_name, kvPair.key, kvPair.value);
    return Response.status(response.status_code).entity(response).build();
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
    if (db_name == null
        || table_name == null
        || kvPair == null
        || kvPair.key == null
        || kvPair.value != null) {

      KVResponse kvResponse =
          new KVResponse(
              400, "Bad request, must provide valid database, table name and key value pair.");
      return Response.status(kvResponse.status_code).entity(kvResponse).build();
    }
    // m_kvservice.deleteKey(db_id, kvPair.key);
    KVResponse response = kvcassandra.deleteKey(db_name, table_name, kvPair.key);
    return Response.status(response.status_code).entity(response).build();
  }
}
