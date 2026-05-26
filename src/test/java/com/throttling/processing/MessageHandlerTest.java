package com.throttling.processing;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import com.throttling.dlq.DlqProducer;
import com.throttling.legacy.LegacyClient;
import com.throttling.legacy.LegacyResponse;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.ThrottleTimeoutException;
import com.throttling.throttling.TokenBucketService;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MessageHandlerTest {

    TokenBucketService throttle;
    LegacyClient legacy;
    DlqProducer dlq;
    MetricsRegistry metrics;
    MessageHandler handler;

    @BeforeEach
    void setup() {
        throttle = mock(TokenBucketService.class);
        legacy = mock(LegacyClient.class);
        dlq = mock(DlqProducer.class);
        metrics = mock(MetricsRegistry.class);
        handler = new MessageHandler(throttle, legacy, dlq, metrics, 5);
        when(throttle.acquireBlocking()).thenReturn(Uni.createFrom().voidItem());
        when(dlq.send(any(), any(), any(), anyInt())).thenReturn(Uni.createFrom().voidItem());
    }

    private MessageEnvelope env() {
        return new MessageEnvelope("M1", "k1", Instant.now(), 0, null,
            new MessageEnvelope.Metadata(null, "users"), Map.of("a", 1));
    }

    @Test
    void calls_legacy_after_throttle_acquired() {
        when(legacy.send(eq("users"), eq("k1"), any()))
            .thenReturn(Uni.createFrom().item(new LegacyResponse(Map.of("ok", true))));

        handler.handle(env()).await().indefinitely();

        verify(throttle).acquireBlocking();
        verify(legacy).send("users", "k1", Map.of("a", 1));
        verifyNoInteractions(dlq);
    }

    @Test
    void sends_to_dlq_with_circuit_open_reason() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new CircuitBreakerOpenException("open")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.CIRCUIT_OPEN);
    }

    @Test
    void sends_to_dlq_with_legacy_5xx_reason() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new LegacyTransientException(503, "x")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.LEGACY_5XX);
    }

    @Test
    void sends_to_dlq_with_permanent_4xx_reason() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new LegacyPermanentException(400, "x")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.LEGACY_4XX_PERMANENT);
    }

    @Test
    void sends_to_dlq_with_throttle_timeout() {
        when(throttle.acquireBlocking())
            .thenReturn(Uni.createFrom().failure(new ThrottleTimeoutException()));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.THROTTLE_TIMEOUT);
        verifyNoInteractions(legacy);
    }
}
