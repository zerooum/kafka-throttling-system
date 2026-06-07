package com.throttling.processing;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import com.throttling.dlq.DlqProducer;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.ThrottleTimeoutException;
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

    RetryOrchestrator orchestrator;
    DlqProducer dlq;
    MetricsRegistry metrics;
    MessageHandler handler;

    @BeforeEach
    void setup() {
        orchestrator = mock(RetryOrchestrator.class);
        dlq = mock(DlqProducer.class);
        metrics = mock(MetricsRegistry.class);
        handler = new MessageHandler(orchestrator, dlq, metrics);
        when(dlq.send(any(), any(), any(), anyInt())).thenReturn(Uni.createFrom().voidItem());
    }

    private MessageEnvelope env() {
        return new MessageEnvelope("M1", "k1", Instant.now(), 0, null,
            new MessageEnvelope.Metadata(null, "users"), Map.of("a", 1));
    }

    @Test
    void acks_and_counts_success_when_orchestrator_completes() {
        when(orchestrator.execute(any())).thenReturn(Uni.createFrom().voidItem());

        handler.handle(env()).await().indefinitely();

        verify(metrics).consumed("success");
        verifyNoInteractions(dlq);
    }

    @Test
    void sends_to_dlq_with_circuit_open_reason() {
        when(orchestrator.execute(any()))
            .thenReturn(Uni.createFrom().failure(new CircuitBreakerOpenException("open")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.CIRCUIT_OPEN);
        verify(metrics, never()).consumed("success");
    }

    @Test
    void sends_to_dlq_with_legacy_5xx_reason() {
        when(orchestrator.execute(any()))
            .thenReturn(Uni.createFrom().failure(new LegacyTransientException(503, "x")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.LEGACY_5XX);
        verify(metrics, never()).consumed("success");
    }

    @Test
    void sends_to_dlq_with_permanent_4xx_reason() {
        when(orchestrator.execute(any()))
            .thenReturn(Uni.createFrom().failure(new LegacyPermanentException(400, "x")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.LEGACY_4XX_PERMANENT);
        verify(metrics, never()).consumed("success");
    }

    @Test
    void sends_to_dlq_with_throttle_timeout_reason() {
        when(orchestrator.execute(any()))
            .thenReturn(Uni.createFrom().failure(new ThrottleTimeoutException()));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.THROTTLE_TIMEOUT);
        verify(metrics, never()).consumed("success");
    }
}
