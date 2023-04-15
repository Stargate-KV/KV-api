package org.stargate.rest.json;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

import javax.ws.rs.core.Response;
import javax.inject.Inject;
import java.util.Map;
import java.util.Collections;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.cql.builder.Replication;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;

// add authorization
@ApplicationScoped
@SecurityRequirement(name = "Token")

@Path("/kvstore/v1")
public class KeyValueResource {
  @Inject
  StargateBridgeClient bridge;
  @Inject
  KVCassandra kvcassandra;
  ObjectMapper objectMapper = new ObjectMapper();

  public KeyValueResource() {

  }

  // create and delete databases
  @POST
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public db_response createDB(String db_name_json) throws KvstoreException, JsonProcessingException {
    JsonNode jsonNode = objectMapper.readTree(db_name_json);
    String db_name = jsonNode.get("db_name").asText();
    kvcassandra.createKeyspace(db_name);
    return new db_response(db_name);
  }

  @DELETE
  @Path("{db_name}")
  public Response deleteDB(@PathParam("db_name") String db_name) throws KvstoreException {
    if (db_name == null) {
      throw new KvstoreException(400, "bad request");
    }
    Response response = kvcassandra.deleteKeyspace(db_name);
    return response;
  }

  // create and delete tables
  @POST
  @Path("databases/{db_name}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public db_response createTable(@PathParam("db_name") String db_name, String table_name_json)
      throws KvstoreException, JsonProcessingException {
    JsonNode jsonNode = objectMapper.readTree(table_name_json);
    String table_name = jsonNode.get("table_name").asText();
    kvcassandra.createTable(db_name, table_name);
    return new db_response(db_name);
  }

  @DELETE
  @Path("{db_name}/{table_name}")
  public Response deleteTable(@PathParam("db_name") String db_name, @PathParam("table_name") String table_name)
      throws KvstoreException {
    if (db_name == null || table_name == null) {
      throw new KvstoreException(400, "bad request");
    }
    Response response = kvcassandra.deleteTable(db_name, table_name);
    return response;
  }

  // list databases and tables
  @GET
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listDBs() throws KvstoreException {
    Response response = kvcassandra.listKeyspaces();
    return response;
  }

  @GET
  @Path("{db_name}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTables(@PathParam("db_name") String db_name) throws KvstoreException {
    if (db_name == null) {
      throw new KvstoreException(400, "bad request");
    }
    Response response = kvcassandra.listTables(db_name);
    return response;
  }

  // put, get, update, and delete key-value pairs
  @PUT
  @Path("{db_name}/{table_name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KeyValPair putKeyVal(@PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name, KeyValPair kvPair) throws KvstoreException {
    if (db_name == null || table_name == null || kvPair == null || kvPair.key == null || kvPair.value == null) {
      throw new KvstoreException(400, "bad request");
    }
    kvcassandra.putKeyVal(db_name, table_name, kvPair.key, kvPair.value);
    return kvPair;
  }

  @GET
  @Path("{db_name}/{table_name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getKeyVal(@PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name, KeyValPair kvPair) throws KvstoreException {
    if (db_name == null || table_name == null || kvPair == null || kvPair.key == null) {
      throw new KvstoreException(400, "bad request");
    }
    kvPair.value = kvcassandra.getVal(db_name, table_name, kvPair.key);
    return Response.status(Response.Status.OK).entity(kvPair).build();
  }

  @PATCH
  @Path("{db_name}/{table_name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KeyValPair updateKeyVal(@PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name, KeyValPair kvPair) throws KvstoreException {
    if (db_name == null || table_name == null || kvPair == null || kvPair.key == null || kvPair.value == null) {
      throw new KvstoreException(400, "bad request");
    }
    // m_kvservice.putKeyVal(db_id, kvPair.key, kvPair.value, true);
    kvcassandra.updateVal(db_name, table_name, kvPair.key, kvPair.value);
    return kvPair;
  }

  @DELETE
  @Path("{db_name}/{table_name}/key")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response deleteKey(@PathParam("db_name") String db_name,
      @PathParam("table_name") String table_name, KeyValPair kvPair) throws KvstoreException {
    if (db_name == null || table_name == null || kvPair == null || kvPair.key == null || kvPair.value != null) {
      throw new KvstoreException(400, "bad request");
    }
    // m_kvservice.deleteKey(db_id, kvPair.key);
    Response response = kvcassandra.deleteKey(db_name, table_name, kvPair.key);
    return response;
  }

}

