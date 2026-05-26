package com.throttling.dlq;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DlqProducerTest {

    @Test
    void sends_dlq_envelope_with_reason_and_error() {
        MutinyEmitter<DlqEnvelope> emitter = mock(MutinyEmitter.class);
        when(emitter.send(org.mockito.ArgumentMatchers.any(DlqEnvelope.class)))
            .thenReturn(Uni.createFrom().voidItem());

        DlqProducer producer = new DlqProducer(emitter);
        MessageEnvelope original = new MessageEnvelope(
            "M1", "k1", Instant.now(), 1, null, null, Map.of()
        );

        producer.send(original, FailureReason.LEGACY_5XX, "boom", 3)
            .await().indefinitely();

        ArgumentCaptor<DlqEnvelope> cap = ArgumentCaptor.forClass(DlqEnvelope.class);
        verify(emitter).send(cap.capture());
        assertThat(cap.getValue().failure().reason()).isEqualTo(FailureReason.LEGACY_5XX);
        assertThat(cap.getValue().failure().lastError()).isEqualTo("boom");
        assertThat(cap.getValue().failure().attempts()).isEqualTo(3);
        assertThat(cap.getValue().original().messageId()).isEqualTo("M1");
    }
}
