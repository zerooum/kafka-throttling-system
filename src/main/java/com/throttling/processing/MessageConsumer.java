package com.throttling.processing;

import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MessageConsumer {

    private static final Logger LOG = Logger.getLogger(MessageConsumer.class);

    @Inject MessageHandler handler;

    @Incoming("messages-in")
    public Uni<Void> consume(Message<MessageEnvelope> msg) {
        MessageEnvelope env = msg.getPayload();
        return handler.handle(env)
            .onItemOrFailure().transformToUni((ignored, err) -> {
                if (err != null) {
                    LOG.errorf(err, "Pipeline failed for messageId=%s; nack", env.messageId());
                    return Uni.createFrom().completionStage(msg.nack(err));
                }
                return Uni.createFrom().completionStage(msg.ack());
            });
    }
}
