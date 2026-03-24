package com.netcracker.cdt.ui.rest.v2;

import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
@Path("/esc/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EscController {

    @ConfigProperty(name = "cdt.version", defaultValue="0.0.1")
    String VERSION;

    @GET
    @Path("/version")
    public String getVersion() {
        return VERSION;
    }
}
