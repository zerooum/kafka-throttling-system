package com.throttling.processing;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import com.throttling.common.MessageEnvelope;
import com.throttling.legacy.LegacyClient;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.TokenBucketService;
import com.throttling.verification.VerificationStore;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.rest.client.inject.RestClient;

@ApplicationScoped
public class RetryOrchestrator {

    private final TokenBucketService throttle;
    private final LegacyClient legacy;
    private final VerificationStore verify;
    private final BackoffPolicy backoff;
    private final MetricsRegistry metrics;
    private final int maxAttempts;

    @Inject
    public RetryOrchestrator(TokenBucketService throttle,
                             @RestClient LegacyClient legacy,
                             VerificationStore verify,
                             BackoffPolicy backoff,
                             MetricsRegistry metrics,
                             VerifyConfig config) {
        this(throttle, legacy, verify, backoff, metrics, config.maxAttempts());
    }

    public RetryOrchestrator(TokenBucketService throttle,
                             LegacyClient legacy,
                             VerificationStore verify,
                             BackoffPolicy backoff,
                             MetricsRegistry metrics,
                             int maxAttempts) {
        this.throttle = throttle;
        this.legacy = legacy;
        this.verify = verify;
        this.backoff = backoff;
        this.metrics = metrics;
        this.maxAttempts = maxAttempts;
    }

    public Uni<Void> execute(MessageEnvelope env) {
        // Capture the Vert.x context at subscription time (the consumer subscribes on the
        // event loop). delayIt() later resumes on a timer thread, so we use this captured
        // context to hop back before touching Hibernate Reactive. Null when subscribed off
        // any Vert.x context (e.g. plain unit tests), in which case no hop is needed.
        return Uni.createFrom().item(() -> Vertx.currentContext())
            .chain(ctx -> attempt(env, 1, ctx));
    }

    private Uni<Void> attempt(MessageEnvelope env, int n, Context ctx) {
        String endpoint = env.metadata() != null && env.metadata().targetEndpoint() != null
            ? env.metadata().targetEndpoint()
            : "default";
        return throttle.acquireBlocking()
            .chain(() -> legacy.send(endpoint, env.idempotencyKey(), env.payload()))
            .replaceWithVoid()
            .onFailure().recoverWithUni(err -> handleFailure(env, n, ctx, err));
    }

    private Uni<Void> handleFailure(MessageEnvelope env, int n, Context ctx, Throwable err) {
        if (!isRetriable(err)) {
            return Uni.createFrom().failure(err);
        }
        Duration delay = backoff.delayForAttempt(n);
        Uni<Void> waited = Uni.createFrom().voidItem().onItem().delayIt().by(delay);
        if (ctx != null) {
            // delayIt() resumes on a non-event-loop timer thread; hop back onto the captured
            // Vert.x (duplicated) context so Hibernate Reactive can open a session in
            // verify.exists() without throwing HR000068.
            waited = waited.emitOn(command -> ctx.runOnContext(v -> command.run()));
        }
        return waited
            .chain(() -> verify.exists(env.idempotencyKey()))
            .chain(found -> {
                if (Boolean.TRUE.equals(found)) {
                    if (metrics != null) metrics.verifyChecked("found");
                    return Uni.createFrom().voidItem();
                }
                if (metrics != null) metrics.verifyChecked("empty");
                if (n < maxAttempts) {
                    if (metrics != null) metrics.apiRetried();
                    return attempt(env, n + 1, ctx);
                }
                return Uni.createFrom().<Void>failure(err);
            });
    }

    /**
     * Retriable failures are legacy-API timeouts and transient 5xx — these may have been
     * processed despite the error, so we wait and check the table before retrying.
     * Everything else (permanent 4xx, circuit-open, throttle-timeout) fails fast: the call
     * either definitively failed or never reached the legado, so there is nothing to verify.
     */
    private boolean isRetriable(Throwable err) {
        Throwable t = err;
        while (t != null) {
            if (t instanceof LegacyTransientException
                || t instanceof TimeoutException
                || t instanceof org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException) {
                return true;
            }
            if (t == t.getCause()) break;
            t = t.getCause();
        }
        return false;
    }
}
