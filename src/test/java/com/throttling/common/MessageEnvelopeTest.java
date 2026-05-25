package com.throttling.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MessageEnvelopeTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void serialises_and_deserialises_round_trip() throws Exception {
        MessageEnvelope env = new MessageEnvelope(
            "01HXYZ",
            "key-1",
            Instant.parse("2026-05-25T14:30:00Z"),
            0,
            new MessageEnvelope.TraceContext("00-abc-def-01", null),
            new MessageEnvelope.Metadata("source-x", "endpoint-y"),
            Map.of("foo", "bar")
        );

        String json = mapper.writeValueAsString(env);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

        assertThat(parsed.messageId()).isEqualTo("01HXYZ");
        assertThat(parsed.idempotencyKey()).isEqualTo("key-1");
        assertThat(parsed.attempt()).isZero();
        assertThat(parsed.traceContext().traceparent()).isEqualTo("00-abc-def-01");
        assertThat(parsed.metadata().targetEndpoint()).isEqualTo("endpoint-y");
        assertThat(parsed.payload()).containsEntry("foo", "bar");
    }
}
