package com.throttling.processing;

import com.throttling.common.MessageEnvelope;
import com.throttling.legacy.LegacyClient;
import com.throttling.legacy.LegacyResponse;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.TokenBucketService;
import com.throttling.verification.VerificationStore;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RetryOrchestratorTest {

    TokenBucketService throttle;
    LegacyClient legacy;
    VerificationStore verify;
    BackoffPolicy backoff;
    MetricsRegistry metrics;
    RetryOrchestrator orchestrator;

    @BeforeEach
    void setup() {
        throttle = mock(TokenBucketService.class);
        legacy = mock(LegacyClient.class);
        verify = mock(VerificationStore.class);
        backoff = mock(BackoffPolicy.class);
        metrics = mock(MetricsRegistry.class);
        when(throttle.acquireBlocking()).thenReturn(Uni.createFrom().voidItem());
        when(backoff.delayForAttempt(anyInt())).thenReturn(Duration.ofMillis(1));
        orchestrator = new RetryOrchestrator(throttle, legacy, verify, backoff, metrics, 3);
    }

    private MessageEnvelope env() {
        return new MessageEnvelope("M1", "k1", Instant.now(), 0, null,
            new MessageEnvelope.Metadata(null, "users"), Map.of("a", 1));
    }

    private Uni<LegacyResponse> ok() {
        return Uni.createFrom().item(new LegacyResponse(Map.of("ok", true)));
    }

    private Uni<LegacyResponse> timeout() {
        return Uni.createFrom().failure(new TimeoutException("timed out"));
    }

    @Test
    void success_on_first_call_acks_without_checking_table() {
        when(legacy.send(eq("users"), eq("k1"), any())).thenReturn(ok());

        orchestrator.execute(env()).await().indefinitely();

        verify(legacy, times(1)).send(any(), any(), any());
        verifyNoInteractions(verify);
    }

    @Test
    void timeout_then_record_found_acks() {
        when(legacy.send(any(), any(), any())).thenReturn(timeout());
        when(verify.exists("k1")).thenReturn(Uni.createFrom().item(true));

        orchestrator.execute(env()).await().indefinitely();

        verify(legacy, times(1)).send(any(), any(), any());
        verify(verify, times(1)).exists("k1");
        verify(metrics).verifyChecked("found");
    }

    @Test
    void transient_5xx_then_record_found_acks() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new LegacyTransientException(503, "boom")));
        when(verify.exists("k1")).thenReturn(Uni.createFrom().item(true));

        orchestrator.execute(env()).await().indefinitely();

        verify(legacy, times(1)).send(any(), any(), any());
        verify(verify, times(1)).exists("k1");
    }

    @Test
    void timeout_three_times_with_empty_table_exhausts_and_fails() {
        when(legacy.send(any(), any(), any())).thenReturn(timeout());
        when(verify.exists("k1")).thenReturn(Uni.createFrom().item(false));

        assertThatThrownBy(() -> orchestrator.execute(env()).await().indefinitely())
            .isInstanceOf(TimeoutException.class);

        verify(legacy, times(3)).send(any(), any(), any());
        verify(verify, times(3)).exists("k1");
        verify(throttle, times(3)).acquireBlocking();
        verify(metrics, times(2)).apiRetried();
    }

    @Test
    void wrapped_timeout_is_unwrapped_and_treated_as_retriable() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("wrapper", new TimeoutException("inner"))));
        when(verify.exists("k1")).thenReturn(Uni.createFrom().item(true));

        orchestrator.execute(env()).await().indefinitely();

        verify(legacy, times(1)).send(any(), any(), any());
        verify(verify, times(1)).exists("k1");
        verify(metrics).verifyChecked("found");
    }

    @Test
    void permanent_4xx_fails_immediately_without_wait_or_check() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new LegacyPermanentException(400, "bad")));

        assertThatThrownBy(() -> orchestrator.execute(env()).await().indefinitely())
            .isInstanceOf(LegacyPermanentException.class);

        verify(legacy, times(1)).send(any(), any(), any());
        verifyNoInteractions(verify);
        verifyNoInteractions(backoff);
    }

    @Test
    void throttle_timeout_fails_fast_without_calling_legacy_or_verify() {
        when(throttle.acquireBlocking())
            .thenReturn(Uni.createFrom().failure(new com.throttling.throttling.ThrottleTimeoutException()));

        assertThatThrownBy(() -> orchestrator.execute(env()).await().indefinitely())
            .isInstanceOf(com.throttling.throttling.ThrottleTimeoutException.class);

        verifyNoInteractions(legacy);
        verifyNoInteractions(verify);
        verifyNoInteractions(backoff);
    }

    @Test
    void circuit_open_fails_fast_without_wait_or_check() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(
                new org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException("open")));

        assertThatThrownBy(() -> orchestrator.execute(env()).await().indefinitely())
            .isInstanceOf(org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException.class);

        verify(legacy, times(1)).send(any(), any(), any());
        verifyNoInteractions(verify);
        verifyNoInteractions(backoff);
    }
}
