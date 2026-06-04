package com.throttling.processing;

import java.util.concurrent.TimeoutException;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import com.throttling.dlq.DlqProducer;
import com.throttling.legacy.LegacyClient;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.ThrottleTimeoutException;
import com.throttling.throttling.TokenBucketService;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MessageHandler {

    private final TokenBucketService throttle;
    private final LegacyClient legacy;
    private final DlqProducer dlq;
    private final MetricsRegistry metrics;
    private final int maxHardRetries;

    @Inject
    public MessageHandler(TokenBucketService throttle,
                          @org.eclipse.microprofile.rest.client.inject.RestClient LegacyClient legacy,
                          DlqProducer dlq,
                          MetricsRegistry metrics,
                          @ConfigProperty(name = "consumer.max-hard-retries", defaultValue = "5") int maxHardRetries) {
        this.throttle = throttle;
        this.legacy = legacy;
        this.dlq = dlq;
        this.metrics = metrics;
        this.maxHardRetries = maxHardRetries;
    }

    public Uni<Void> handle(MessageEnvelope env) {
        String endpoint = env.metadata() != null && env.metadata().targetEndpoint() != null
            ? env.metadata().targetEndpoint()
            : "default";

        return throttle.acquireBlocking()
            .chain(() -> legacy.send(endpoint, env.idempotencyKey(), env.payload()))
            .invoke(() -> { if (metrics != null) metrics.consumed("success"); })
            .replaceWithVoid()
            .onFailure().recoverWithUni(err -> sendToDlq(env, err));
    }

    private Uni<Void> sendToDlq(MessageEnvelope env, Throwable err) {
        FailureReason reason = classify(err);
        if (metrics != null) { metrics.consumed("dlq"); metrics.dlqSent(reason.name()); }
        return dlq.send(env, reason, err.getMessage(), env.attempt());
    }

    private FailureReason classify(Throwable err) {
        Throwable t = unwrap(err);
        if (t instanceof CircuitBreakerOpenException) return FailureReason.CIRCUIT_OPEN;
        if (t instanceof ThrottleTimeoutException) return FailureReason.THROTTLE_TIMEOUT;
        if (t instanceof LegacyPermanentException) return FailureReason.LEGACY_4XX_PERMANENT;
        if (t instanceof LegacyTransientException) return FailureReason.LEGACY_5XX;
        if (t instanceof TimeoutException
            || t instanceof org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException) {
            return FailureReason.LEGACY_TIMEOUT;
        }
        return FailureReason.LEGACY_5XX;
    }

    private Throwable unwrap(Throwable t) {
        while (t.getCause() != null && t != t.getCause()) {
            if (t instanceof CircuitBreakerOpenException || t instanceof ThrottleTimeoutException
                || t instanceof LegacyTransientException || t instanceof LegacyPermanentException) {
                return t;
            }
            t = t.getCause();
        }
        return t;
    }

    public int maxHardRetries() { return maxHardRetries; }
}
