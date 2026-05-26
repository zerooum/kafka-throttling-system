package com.throttling.ingress.dto;

import java.time.Instant;

public record IngressResponse(String messageId, Instant acceptedAt) {}
