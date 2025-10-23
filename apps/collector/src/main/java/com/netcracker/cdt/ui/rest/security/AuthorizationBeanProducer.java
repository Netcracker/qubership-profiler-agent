package com.netcracker.cdt.ui.rest.security;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import io.quarkus.vertx.http.runtime.security.HttpSecurityPolicy;
import io.quarkus.vertx.http.runtime.security.PermitSecurityPolicy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
public class AuthorizationBeanProducer {
    @ConfigProperty(name = "quarkus.http.auth.basic")
    Optional<Boolean> basic;

    @ConfigProperty(name = "quarkus.oidc.enabled")
    Optional<Boolean> oauth;

    @Produces
    public HttpSecurityPolicy getAuthorization() {
        Boolean basicEnabled = basic.orElse(false);
        Boolean oauthEnabled = oauth.orElse(false);

        if (basicEnabled || oauthEnabled) {
            Log.infof("Authorization is enabled");
            return new Authorization();
        }

        Log.infof("Authorization is not enabled");
        return new PermitSecurityPolicy();
    }
}
