package com.throttling.legacy;

import io.smallrye.faulttolerance.api.CircuitBreakerName;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.HeaderParam;
import org.eclipse.microprofile.faulttolerance.Bulkhead;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import com.throttling.legacy.exceptions.LegacyTransientException;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@RegisterRestClient(configKey = "legacy-api")
@RegisterProvider(LegacyResponseMapper.class)
@Path("/")
public interface LegacyClient {

    @POST
    @Path("{endpoint}")
    @CircuitBreaker(
        requestVolumeThreshold = 20,
        failureRatio = 0.5,
        delay = 10, delayUnit = ChronoUnit.SECONDS,
        successThreshold = 3,
        failOn = { LegacyTransientException.class, TimeoutException.class })
    @CircuitBreakerName("legacy-client")
    @Timeout(value = 5000)
    @Bulkhead(value = 50)
    Uni<LegacyResponse> send(@PathParam("endpoint") String endpoint,
                             @HeaderParam("Idempotency-Key") String idempKey,
                             Map<String, Object> payload);
}
