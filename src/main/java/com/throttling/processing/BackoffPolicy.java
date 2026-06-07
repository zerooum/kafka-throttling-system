package com.throttling.processing;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;

@ApplicationScoped
public class BackoffPolicy {

    private final Duration base;
    private final int multiplier;

    @Inject
    public BackoffPolicy(VerifyConfig config) {
        this(config.baseDelay(), config.backoffMultiplier());
    }

    public BackoffPolicy(Duration base, int multiplier) {
        this.base = base;
        this.multiplier = multiplier;
    }

    /** Delay before the verification check that follows attempt {@code n} (1-based). */
    public Duration delayForAttempt(int n) {
        long factor = (long) Math.pow(multiplier, n - 1);
        return base.multipliedBy(factor);
    }
}
