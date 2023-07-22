package org.stargate.rest.json;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;

import javax.ws.rs.core.Response;
import javax.inject.Inject;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;


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

  private static KeyValService m_kvservice = new KeyValService();

  public KeyValueResource() {

  }

  @POST
  @Path("/cache/on")
  public Response cacheOn() {
    kvcassandra.cacheOn();
    return Response.ok().entity("Cache is now ON").build();
  }

  @POST
  @Path("/cache/off")
  public Response cacheOff() {
    kvcassandra.cacheOff();
    return Response.ok().entity("Cache is now OFF").build();
  }

  @PUT
  @Path("{db_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KeyValPair putKeyVal(@PathParam("db_id") Integer db_id, KeyValPair kvPair) throws KvstoreException {
    if (db_id == null || kvPair == null || kvPair.key == null || kvPair.value == null) {
      throw new KvstoreException(400, "bad request");
    }
    // m_kvservice.putKeyVal(db_id, kvPair.key, kvPair.value, false);
    kvcassandra.putKeyVal(dbid_to_keyspace(db_id), kvPair.key, kvPair.value);

    return kvPair;
  }

  @GET
  @Path("{db_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getKeyVal(@PathParam("db_id") Integer db_id, KeyValPair kvPair) throws KvstoreException {
    if (db_id == null || kvPair == null || kvPair.key == null) {
      throw new KvstoreException(400, "bad request");
    }
    // String val = m_kvservice.getKey(db_id, kvPair.key);
    kvPair.value = kvcassandra.getVal(dbid_to_keyspace(db_id), kvPair.key);
    // kvPair.value = val;

    return Response.status(Response.Status.OK).entity(kvPair).build();
  }

  @PATCH
  @Path("{db_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KeyValPair updateKeyVal(@PathParam("db_id") Integer db_id, KeyValPair kvPair) throws KvstoreException {
    if (db_id == null || kvPair == null || kvPair.key == null || kvPair.value == null) {
      throw new KvstoreException(400, "bad request");
    }
    // m_kvservice.putKeyVal(db_id, kvPair.key, kvPair.value, true);
    kvcassandra.putKeyVal(dbid_to_keyspace(db_id), kvPair.key, kvPair.value);
    return kvPair;
  }

  @POST
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  public db_response createDB() {
    int db_id = m_kvservice.createDatabase();
    // return new db_response(db_id);
    kvcassandra.createKeyspace(dbid_to_keyspace(db_id));
    return new db_response(db_id);
  }

  @DELETE
  @Path("{db_id}")
  public Response deleteDB(@PathParam("db_id") Integer db_id) throws KvstoreException {
    if (db_id == null) {
      throw new KvstoreException(400, "bad request");
    }
    Response response = kvcassandra.deleteKeyspace(dbid_to_keyspace(db_id));
    // m_kvservice.deleteDB(db_id);
    return response;
  }

  @DELETE
  @Path("{db_id}/key")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response deleteKey(@PathParam("db_id") Integer db_id, KeyValPair kvPair) throws KvstoreException {
    if (db_id == null || kvPair == null || kvPair.key == null || kvPair.value != null) {
      throw new KvstoreException(400, "bad request");
    }
    // m_kvservice.deleteKey(db_id, kvPair.key);
    Response response = kvcassandra.deleteKey(dbid_to_keyspace(db_id), kvPair.key);
    return response;
  }

  private String dbid_to_keyspace(int db_id) {
    return "kvdb" + Integer.toString(db_id);
  }
}

