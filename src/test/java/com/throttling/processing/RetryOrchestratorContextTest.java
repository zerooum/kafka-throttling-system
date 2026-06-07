package com.throttling.processing;

import com.throttling.common.MessageEnvelope;
import com.throttling.integration.PostgresTestResource;
import com.throttling.legacy.LegacyClient;
import com.throttling.throttling.TokenBucketService;
import com.throttling.verification.ProcessingRecord;
import com.throttling.verification.VerificationStore;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Probe: confirms the Vert.x duplicated context (needed by Hibernate Reactive) survives the
 * Mutiny delayIt() hop, so the real PgVerificationStore.exists (Panache.withSession) runs
 * correctly after the backoff. Uses the REAL VerificationStore against a Postgres container;
 * only the throttle and legacy client are mocked.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class RetryOrchestratorContextTest {

    @Inject
    VerificationStore store;

    @Test
    @RunOnVertxContext
    void verify_runs_after_delay_with_live_vertx_context(UniAsserter asserter) {
        String key = "ctx-" + UUID.randomUUID();
        MessageEnvelope env = new MessageEnvelope("M1", key, Instant.now(), 0, null,
            new MessageEnvelope.Metadata(null, "users"), Map.of("a", 1));

        TokenBucketService throttle = mock(TokenBucketService.class);
        when(throttle.acquireBlocking()).thenReturn(Uni.createFrom().voidItem());

        LegacyClient legacy = mock(LegacyClient.class);
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new TimeoutException("simulated timeout")));

        RetryOrchestrator orchestrator = new RetryOrchestrator(
            throttle, legacy, store, new BackoffPolicy(Duration.ofMillis(50), 1), null, 3);

        // The legado "processed" the message despite the timeout: seed its row.
        asserter.execute(() -> Panache.withTransaction(() -> {
            ProcessingRecord rec = new ProcessingRecord();
            rec.idempotencyKey = key;
            rec.processedAt = Instant.now();
            return rec.persist();
        }));

        // execute: timeout -> delay 50ms -> real verify.exists (Panache.withSession) -> found -> ack.
        // If the Vert.x context were lost across delayIt, withSession would throw here and the Uni would fail.
        asserter.assertThat(() -> orchestrator.execute(env).replaceWith(Boolean.TRUE),
            ok -> assertThat(ok).isTrue());
    }
}
