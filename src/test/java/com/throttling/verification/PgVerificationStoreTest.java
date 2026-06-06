package com.throttling.verification;

import com.throttling.integration.PostgresTestResource;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class PgVerificationStoreTest {

    @Inject
    VerificationStore store;

    @Test
    @RunOnVertxContext
    void exists_is_false_when_absent(UniAsserter asserter) {
        asserter.assertThat(() -> store.exists("missing-key"),
            found -> assertThat(found).isFalse());
    }

    @Test
    @RunOnVertxContext
    void exists_is_true_after_record_written(UniAsserter asserter) {
        String key = "present-" + UUID.randomUUID();
        asserter.execute(() -> Panache.withTransaction(() -> {
            ProcessingRecord rec = new ProcessingRecord();
            rec.idempotencyKey = key;
            rec.processedAt = Instant.now();
            return rec.persist();
        }));
        asserter.assertThat(() -> store.exists(key),
            found -> assertThat(found).isTrue());
    }
}
