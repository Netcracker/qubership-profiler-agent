package com.netcracker.cdt.ui.rest.security;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import io.quarkus.runtime.configuration.ConfigurationException;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.credential.PasswordCredential;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.UsernamePasswordAuthenticationRequest;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Arrays;
import java.util.Optional;

@LookupIfProperty(name = "quarkus.http.auth.basic", stringValue = "true")
@ApplicationScoped
public class BasicIdentityProvider implements IdentityProvider<UsernamePasswordAuthenticationRequest> {

    private static final String USERNAME_PARAM = "UI_USERNAME";
    private static final String PASSWORD_PARAM = "UI_PASSWORD";

    @ConfigProperty(name = USERNAME_PARAM)
    Optional<String> configuredUsername;

    @ConfigProperty(name = PASSWORD_PARAM)
    Optional<String> configuredPassword;

    @Override
    public Class<UsernamePasswordAuthenticationRequest> getRequestType() {
        return UsernamePasswordAuthenticationRequest.class;
    }

    @Override
    public Uni<SecurityIdentity> authenticate(UsernamePasswordAuthenticationRequest req, AuthenticationRequestContext ctx) {
        final String username = req.getUsername();
        final PasswordCredential password = req.getPassword();

        final String validUsername = configuredUsername.orElseThrow(() -> new ConfigurationException(USERNAME_PARAM + " property is not defined"));
        final char[] validPassword = configuredPassword.orElseThrow(() -> new ConfigurationException(PASSWORD_PARAM + " property is not defined")).toCharArray();

        if (!(validUsername.equals(username) && Arrays.equals(validPassword, password.getPassword()))) {
            Log.infof("User %s login failed", username);
            throw new AuthenticationFailedException("Credentials are not valid");
        }
        final QuarkusSecurityIdentity identity = QuarkusSecurityIdentity.builder()
                .setPrincipal(new QuarkusPrincipal(username))
                .build();
        Log.infof("User %s logged in successfully", identity.getPrincipal().getName());
        return Uni.createFrom().item(identity);
    }
}
