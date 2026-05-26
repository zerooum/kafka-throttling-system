package com.throttling.ingress;

import com.throttling.common.MessageEnvelope;
import com.throttling.common.UlidGenerator;
import com.throttling.ingress.dto.IngressRequest;
import com.throttling.observability.MetricsRegistry;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class IngressServiceTest {

    IdempotencyStore store;
    MessageProducer producer;
    UlidGenerator ulid;
    MetricsRegistry metrics;
    IngressService service;

    @BeforeEach
    void setup() {
        store = mock(IdempotencyStore.class);
        producer = mock(MessageProducer.class);
        ulid = mock(UlidGenerator.class);
        metrics = mock(MetricsRegistry.class);
        service = new IngressService(store, producer, ulid, metrics, Duration.ofSeconds(60));
        when(ulid.next()).thenReturn("M1");
    }

    @Test
    void accepts_new_message() {
        when(store.tryStore(eq("k1"), eq("M1"), any(Duration.class)))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        when(producer.send(any())).thenReturn(Uni.createFrom().voidItem());

        IngressOutcome out = service.handle(
            "k1",
            null,
            new IngressRequest(Map.of("a", 1), null)
        ).await().indefinitely();

        assertThat(out).isInstanceOf(IngressOutcome.Accepted.class);
        ArgumentCaptor<MessageEnvelope> cap = ArgumentCaptor.forClass(MessageEnvelope.class);
        verify(producer).send(cap.capture());
        assertThat(cap.getValue().messageId()).isEqualTo("M1");
        assertThat(cap.getValue().idempotencyKey()).isEqualTo("k1");
    }

    @Test
    void returns_duplicate_when_key_already_present() {
        when(store.tryStore(eq("k1"), eq("M1"), any(Duration.class)))
            .thenReturn(Uni.createFrom().item(Optional.of("M_ORIGINAL")));

        IngressOutcome out = service.handle(
            "k1",
            null,
            new IngressRequest(Map.of("a", 1), null)
        ).await().indefinitely();

        assertThat(out).isEqualTo(new IngressOutcome.Duplicate("M_ORIGINAL"));
        verifyNoInteractions(producer);
    }

    @Test
    void removes_idempotency_key_when_kafka_send_fails() {
        when(store.tryStore(eq("k1"), eq("M1"), any(Duration.class)))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        when(producer.send(any()))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("boom")));
        when(store.remove("k1")).thenReturn(Uni.createFrom().voidItem());

        try {
            service.handle("k1", null, new IngressRequest(Map.of(), null))
                .await().indefinitely();
        } catch (Exception expected) { /* ok */ }

        verify(store).remove("k1");
    }
}
