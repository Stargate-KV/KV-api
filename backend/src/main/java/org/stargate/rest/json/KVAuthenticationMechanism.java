package org.stargate.rest.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.security.HeaderAuthenticationRequest;
import io.stargate.sgv2.api.common.security.HeaderBasedAuthenticationMechanism;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVAuthenticationMechanism extends HeaderBasedAuthenticationMechanism {

  private static final Logger LOG = LoggerFactory.getLogger(KVAuthenticationMechanism.class);

  /** The name of the header to be used for the authentication. */
  private final String headerName;

  /** Object mapper for custom response. */
  private final ObjectMapper objectMapper;

  public KVAuthenticationMechanism(String headerName, ObjectMapper objectMapper) {
    super(headerName, objectMapper);
    this.headerName = headerName;
    this.objectMapper = objectMapper;
  }

  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    String token = context.request().getHeader(headerName);

    if (null != token) {
      HeaderAuthenticationRequest request = new HeaderAuthenticationRequest(headerName, token);
      HttpSecurityUtils.setRoutingContextAttribute(request, context);
      return identityProviderManager.authenticate(request);
    }

    // No suitable header has been found in this request,
    return Uni.createFrom().optional(Optional.empty());
  }
}
