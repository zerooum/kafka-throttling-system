package com.throttling.common;

public enum FailureReason {
    LEGACY_5XX(false),
    LEGACY_TIMEOUT(false),
    LEGACY_4XX_PERMANENT(true),
    CIRCUIT_OPEN(false),
    THROTTLE_TIMEOUT(false),
    INVALID_PAYLOAD(true),
    HARD_RETRY_LIMIT(true);

    private final boolean permanent;

    FailureReason(boolean permanent) {
        this.permanent = permanent;
    }

    public boolean isPermanent() {
        return permanent;
    }
}
