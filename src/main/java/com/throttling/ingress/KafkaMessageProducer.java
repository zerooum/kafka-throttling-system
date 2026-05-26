package com.throttling.ingress;

import com.throttling.common.Constants;
import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

@ApplicationScoped
public class KafkaMessageProducer implements MessageProducer {

    private final MutinyEmitter<MessageEnvelope> emitter;

    @Inject
    public KafkaMessageProducer(@Channel("messages-out") MutinyEmitter<MessageEnvelope> emitter) {
        this.emitter = emitter;
    }

    @Override
    public Uni<Void> send(MessageEnvelope envelope) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(Constants.HEADER_MESSAGE_ID, envelope.messageId().getBytes());
        if (envelope.traceContext() != null && envelope.traceContext().traceparent() != null) {
            headers.add(Constants.HEADER_TRACEPARENT, envelope.traceContext().traceparent().getBytes());
            if (envelope.traceContext().tracestate() != null) {
                headers.add(Constants.HEADER_TRACESTATE, envelope.traceContext().tracestate().getBytes());
            }
        }
        OutgoingKafkaRecordMetadata<String> meta = OutgoingKafkaRecordMetadata.<String>builder()
            .withKey(envelope.messageId())
            .withTopic(Constants.TOPIC_MESSAGES_IN)
            .withHeaders(headers)
            .build();
        Message<MessageEnvelope> msg = Message.of(envelope, Metadata.of(meta));
        return emitter.sendMessage(msg);
    }
}
