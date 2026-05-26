package com.throttling.ingress;

import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;

public interface MessageProducer {
    Uni<Void> send(MessageEnvelope envelope);
}
