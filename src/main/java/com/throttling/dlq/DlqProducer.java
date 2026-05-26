package com.throttling.dlq;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class DlqProducer {

    private final MutinyEmitter<DlqEnvelope> emitter;

    @Inject
    public DlqProducer(@Channel("messages-dlq") MutinyEmitter<DlqEnvelope> emitter) {
        this.emitter = emitter;
    }

    @WithSpan("dlq.send")
    public Uni<Void> send(MessageEnvelope original,
                          @SpanAttribute("dlq.reason") FailureReason reason,
                          String err,
                          int attempts) {
        return emitter.send(DlqEnvelope.of(original, reason, err, attempts));
    }
}
