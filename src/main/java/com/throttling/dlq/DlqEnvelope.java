package com.throttling.dlq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record DlqEnvelope(
        MessageEnvelope original,
        Failure failure
) {
    public record Failure(
        FailureReason reason,
        String lastError,
        int attempts,
        Instant failedAt
    ) {}

    public static DlqEnvelope of(MessageEnvelope original, FailureReason reason, String err, int attempts) {
        return new DlqEnvelope(original, new Failure(reason, err, attempts, Instant.now()));
    }
}
