package com.throttling.ingress.dto;

public record DuplicateResponse(String error, String messageId, String idempotencyKey) {
    public static DuplicateResponse of(String messageId, String idempotencyKey) {
        return new DuplicateResponse("DUPLICATE", messageId, idempotencyKey);
    }
}
