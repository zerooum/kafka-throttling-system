package com.throttling.ingress;

import java.time.Instant;

public sealed interface IngressOutcome {
    record Accepted(String messageId, Instant acceptedAt) implements IngressOutcome {}
    record Duplicate(String originalMessageId) implements IngressOutcome {}
}
