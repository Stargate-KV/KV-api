package org.stargate.rest.json;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class KVExceptionMapper implements ExceptionMapper<KvstoreException> {
  @Override
  public Response toResponse(KvstoreException exception) {
    return Response.status(exception.error_code).entity(exception).build();
  }
}
