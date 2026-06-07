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
        String behavior = resolveBehavior(payload);
        LOG.debugf("mock-legacy: endpoint=%s idempKey=%s behavior=%s", endpoint, idempKey, behavior);

        // "error": legacy did NOT process — no record written, transient 5xx so the
        // orchestrator retries, finds the table empty, and eventually routes to DLQ.
        if ("error".equals(behavior)) {
            return Uni.createFrom().item(
                Response.status(503).entity(Map.of("status", "error", "endpoint", endpoint)).build());
        }

        // "normal" and "timeout" both mean the legacy processed the request -> write the record.
        Response ok = Response.ok(Map.of("status", "ok", "endpoint", endpoint)).build();
        boolean slow = "timeout".equals(behavior);
        return writeRecord(idempKey)
            .chain(() -> slow
                ? Uni.createFrom().item(ok).onItem().delayIt().by(Duration.ofSeconds(7))
                : Uni.createFrom().item(ok));
    }

    /**
     * Selects the simulated legacy behavior: "normal" (200, processed), "timeout"
     * (processed but responds past the client timeout) or "error" (transient 5xx, not
     * processed). Defaults to "normal"; honors the legacy {@code simulateTimeout} flag.
     */
    private String resolveBehavior(Map<String, Object> payload) {
        if (payload == null) return "normal";
        Object raw = payload.get("legacyBehavior");
        if (raw instanceof String s) {
            String b = s.trim().toLowerCase();
            if (b.equals("timeout") || b.equals("error")) return b;
            if (b.equals("normal")) return "normal";
        }
        if (Boolean.TRUE.equals(payload.get("simulateTimeout"))) return "timeout";
        return "normal";
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
