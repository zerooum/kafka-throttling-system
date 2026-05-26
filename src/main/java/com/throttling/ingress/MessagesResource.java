package com.throttling.ingress;

import com.throttling.common.Constants;
import com.throttling.common.MessageEnvelope;
import com.throttling.ingress.dto.DuplicateResponse;
import com.throttling.ingress.dto.IngressRequest;
import com.throttling.ingress.dto.IngressResponse;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MessagesResource {

    @Inject IngressService service;

    @POST
    public Uni<Response> postMessage(
            @HeaderParam(Constants.HEADER_IDEMP_KEY) String idempotencyKey,
            @HeaderParam(Constants.HEADER_TRACEPARENT) String traceparent,
            @HeaderParam(Constants.HEADER_TRACESTATE) String tracestate,
            @Valid IngressRequest request) {

        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return Uni.createFrom().item(Response
                .status(Response.Status.BAD_REQUEST)
                .entity("{\"error\":\"X-Idempotency-Key header required\"}")
                .build());
        }
        if (request == null || request.payload() == null) {
            return Uni.createFrom().item(Response
                .status(Response.Status.BAD_REQUEST)
                .entity("{\"error\":\"payload required\"}")
                .build());
        }

        MessageEnvelope.TraceContext tc = traceparent != null
            ? new MessageEnvelope.TraceContext(traceparent, tracestate)
            : null;

        return service.handle(idempotencyKey, tc, request)
            .map(outcome -> switch (outcome) {
                case IngressOutcome.Accepted a ->
                    Response.accepted(new IngressResponse(a.messageId(), a.acceptedAt())).build();
                case IngressOutcome.Duplicate d ->
                    Response.status(Response.Status.CONFLICT)
                        .entity(DuplicateResponse.of(d.originalMessageId(), idempotencyKey))
                        .build();
            });
    }
}
