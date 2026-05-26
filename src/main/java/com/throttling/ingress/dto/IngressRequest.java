package com.throttling.ingress.dto;

import com.throttling.common.MessageEnvelope;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public record IngressRequest(
        @NotNull Map<String, Object> payload,
        MessageEnvelope.Metadata metadata
) {}
