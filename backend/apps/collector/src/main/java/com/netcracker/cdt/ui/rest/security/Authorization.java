package com.netcracker.cdt.ui.rest.security;

import io.quarkus.logging.Log;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.vertx.http.runtime.security.HttpSecurityPolicy;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import java.util.List;

public class Authorization implements HttpSecurityPolicy {
    private final List<String> permittedPaths = List.of(
            "/q/health/live",
            "/q/health/ready",
            "/q/metrics"
    );

    @Override
    public Uni<CheckResult> checkPermission(RoutingContext request, Uni<SecurityIdentity> identity, AuthorizationRequestContext requestContext) {
        String path = request.request().path();
        return identity.onItem().transform(securityIdentity -> {
            if (permittedPaths.contains(path)) {
                return CheckResult.PERMIT;
            }
            if (securityIdentity.isAnonymous()) {
                return CheckResult.DENY;
            } else {
                return CheckResult.PERMIT;
            }
        });
    }
}
