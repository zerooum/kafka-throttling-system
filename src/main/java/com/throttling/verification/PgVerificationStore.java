package com.throttling.verification;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PgVerificationStore implements VerificationStore {

    @Override
    public Uni<Boolean> exists(String idempotencyKey) {
        return Panache.withSession(() ->
                ProcessingRecord.count("idempotencyKey", idempotencyKey))
            .map(count -> count > 0);
    }
}
