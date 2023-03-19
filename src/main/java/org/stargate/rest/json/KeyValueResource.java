package org.stargate.rest.json;

import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;

// add authorization
@ApplicationScoped
@SecurityRequirement(name = "Token")
@Path("/kvstore/v1")
public class KeyValueResource {
  @Inject StargateBridgeClient bridge;
  @Inject KVCassandra kvcassandra;

  private static KeyValService m_kvservice = new KeyValService();

  public KeyValueResource() {}

  @PUT
  @Path("{db_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KeyValPair putKeyVal(@PathParam("db_id") Integer db_id, KeyValPair kvPair)
      throws KvstoreException {
    if (db_id == null || kvPair == null || kvPair.key == null || kvPair.value == null) {
      throw new KvstoreException(400, "bad request");
    }
    m_kvservice.putKeyVal(db_id, kvPair.key, kvPair.value, false);
    kvcassandra.putKeyVal(Integer.toString(db_id), kvPair.key, kvPair.value);
    return kvPair;
  }

  @GET
  @Path("{db_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KeyValPair getKeyVal(@PathParam("db_id") Integer db_id, KeyValPair kvPair)
      throws KvstoreException {
    if (db_id == null || kvPair == null || kvPair.key == null) {
      throw new KvstoreException(400, "bad request");
    }
    // String val = m_kvservice.getKey(db_id, kvPair.key);
    kvPair.value = kvcassandra.getVal(Integer.toString(db_id), kvPair.key);
    // kvPair.value = val;

    return kvPair;
  }

  @PATCH
  @Path("{db_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public KeyValPair updateKeyVal(@PathParam("db_id") Integer db_id, KeyValPair kvPair)
      throws KvstoreException {
    if (db_id == null || kvPair == null || kvPair.key == null || kvPair.value == null) {
      throw new KvstoreException(400, "bad request");
    }
    m_kvservice.putKeyVal(db_id, kvPair.key, kvPair.value, true);
    return kvPair;
  }

  @POST
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  public db_response createDB() {
    int db_id = m_kvservice.createDatabase();
    // return new db_response(db_id);
    kvcassandra.createKeyspace(db_id);
    return new db_response(db_id);
  }

  @DELETE
  @Path("{db_id}")
  public void deleteDB(@PathParam("db_id") Integer db_id) throws KvstoreException {
    if (db_id == null) {
      throw new KvstoreException(400, "bad request");
    }
    m_kvservice.deleteDB(db_id);
  }

  @DELETE
  @Path("{db_id}/key")
  @Consumes(MediaType.APPLICATION_JSON)
  public void deleteKey(@PathParam("db_id") Integer db_id, KeyValPair kvPair)
      throws KvstoreException {
    if (db_id == null || kvPair == null || kvPair.key == null || kvPair.value != null) {
      throw new KvstoreException(400, "bad request");
    }
    m_kvservice.deleteKey(db_id, kvPair.key);
  }
}
