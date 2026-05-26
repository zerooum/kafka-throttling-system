package com.throttling.admin;

import com.throttling.throttling.BucketConfig;
import com.throttling.throttling.TokenBucketService;
import io.smallrye.faulttolerance.api.CircuitBreakerMaintenance;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;

@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
public class AdminResource {

    @Inject TokenBucketService bucket;
    @Inject BucketConfig cfg;
    @Inject CircuitBreakerMaintenance cb;

    @ConfigProperty(name = "admin.token") String adminToken;

    private Response unauthorized() {
        return Response.status(401).entity(Map.of("error", "unauthorized")).build();
    }

    private boolean authed(String token) {
        return adminToken.equals(token);
    }

    @GET
    @Path("/throttle")
    public Uni<Response> throttle(@HeaderParam("X-Admin-Token") String token) {
        if (!authed(token)) return Uni.createFrom().item(unauthorized());
        return bucket.availableTokens().map(avail -> Response.ok(Map.of(
            "capacity", cfg.capacity(),
            "available", avail,
            "refillTokens", cfg.refillTokens(),
            "refillPeriodMs", cfg.refillPeriodMs()
        )).build());
    }

    @GET
    @Path("/circuit-breaker/status")
    public Response cbStatus(@HeaderParam("X-Admin-Token") String token) {
        if (!authed(token)) return unauthorized();
        return Response.ok(Map.of("state", cb.currentState("legacy-client").name())).build();
    }

    @POST
    @Path("/circuit-breaker/reset")
    public Response cbReset(@HeaderParam("X-Admin-Token") String token) {
        if (!authed(token)) return unauthorized();
        cb.reset("legacy-client");
        return Response.ok(Map.of("state", "RESET")).build();
    }
}
