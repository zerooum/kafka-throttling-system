package com.throttling.verification;

import io.smallrye.mutiny.Uni;

public interface VerificationStore {
    /** True when the legacy recorded processing for this idempotency key. */
    Uni<Boolean> exists(String idempotencyKey);
}
