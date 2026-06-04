package com.throttling.legacy;

import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.Map;

@Path("/dev-legacy")
@ApplicationScoped
@IfBuildProfile("dev")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MockLegacyResource {

    private static final Logger LOG = Logger.getLogger(MockLegacyResource.class);

    @POST
    @Path("{endpoint}")
    public Uni<Response> handle(
            @PathParam("endpoint") String endpoint,
            @HeaderParam("Idempotency-Key") String idempKey,
            Map<String, Object> payload) {
        LOG.debugf("mock-legacy: endpoint=%s idempKey=%s", endpoint, idempKey);
        return Uni.createFrom().item(
            Response.ok(Map.of("status", "ok", "endpoint", endpoint)).build()
        );
    }
}
