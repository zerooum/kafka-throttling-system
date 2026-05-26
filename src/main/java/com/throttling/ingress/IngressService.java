package com.throttling.ingress;

import com.throttling.common.MessageEnvelope;
import com.throttling.common.UlidGenerator;
import com.throttling.ingress.dto.IngressRequest;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.time.Instant;

@ApplicationScoped
public class IngressService {

    private final IdempotencyStore store;
    private final MessageProducer producer;
    private final UlidGenerator ulid;
    private final Duration idempotencyTtl;

    @Inject
    public IngressService(IdempotencyStore store,
                          MessageProducer producer,
                          UlidGenerator ulid,
                          @ConfigProperty(name = "ingress.idempotency.ttl-seconds", defaultValue = "86400") long ttlSeconds) {
        this(store, producer, ulid, Duration.ofSeconds(ttlSeconds));
    }

    public IngressService(IdempotencyStore store,
                          MessageProducer producer,
                          UlidGenerator ulid,
                          Duration idempotencyTtl) {
        this.store = store;
        this.producer = producer;
        this.ulid = ulid;
        this.idempotencyTtl = idempotencyTtl;
    }

    public Uni<IngressOutcome> handle(String idempotencyKey,
                                      MessageEnvelope.TraceContext traceContext,
                                      IngressRequest request) {
        final String messageId = ulid.next();
        final Instant now = Instant.now();
        final MessageEnvelope envelope = new MessageEnvelope(
            messageId,
            idempotencyKey,
            now,
            0,
            traceContext,
            request.metadata(),
            request.payload()
        );

        return store.tryStore(idempotencyKey, messageId, idempotencyTtl)
            .onItem().transformToUni(existing -> {
                if (existing.isPresent()) {
                    return Uni.createFrom().item(new IngressOutcome.Duplicate(existing.get()));
                }
                return producer.send(envelope)
                    .onFailure().call(err -> store.remove(idempotencyKey))
                    .replaceWith(new IngressOutcome.Accepted(messageId, now));
            });
    }
}
