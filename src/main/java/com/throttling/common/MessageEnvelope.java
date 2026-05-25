package com.throttling.common;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MessageEnvelope(
        String messageId,
        String idempotencyKey,
        Instant occurredAt,
        int attempt,
        TraceContext traceContext,
        Metadata metadata,
        Map<String, Object> payload
) {
    public MessageEnvelope withAttempt(int newAttempt) {
        return new MessageEnvelope(messageId, idempotencyKey, occurredAt, newAttempt, traceContext, metadata, payload);
    }

    public record TraceContext(String traceparent, String tracestate) {}

    public record Metadata(String source, String targetEndpoint) {}
}
