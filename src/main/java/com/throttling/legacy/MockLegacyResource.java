package com.throttling.legacy;

import com.throttling.verification.ProcessingRecord;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.hibernate.reactive.panache.Panache;
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

import java.time.Duration;
import java.time.Instant;
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
        boolean slow = payload != null && Boolean.TRUE.equals(payload.get("simulateTimeout"));
        LOG.debugf("mock-legacy: endpoint=%s idempKey=%s simulateTimeout=%s", endpoint, idempKey, slow);

        Response ok = Response.ok(Map.of("status", "ok", "endpoint", endpoint)).build();
        return writeRecord(idempKey)
            .chain(() -> slow
                ? Uni.createFrom().item(ok).onItem().delayIt().by(Duration.ofSeconds(7))
                : Uni.createFrom().item(ok));
    }

    /** Simulates the legacy system persisting its processing record, keyed by the idempotency key. */
    private Uni<Void> writeRecord(String idempKey) {
        if (idempKey == null) return Uni.createFrom().voidItem();
        return Panache.withTransaction(() ->
            ProcessingRecord.<ProcessingRecord>findById(idempKey)
                .chain(existing -> {
                    if (existing != null) return Uni.createFrom().voidItem();
                    ProcessingRecord rec = new ProcessingRecord();
                    rec.idempotencyKey = idempKey;
                    rec.processedAt = Instant.now();
                    return rec.persist().replaceWithVoid();
                }));
    }
}
