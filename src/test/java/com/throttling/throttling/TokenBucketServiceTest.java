package com.throttling.throttling;

import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.distributed.AsyncBucketProxy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TokenBucketServiceTest {

    @Test
    void completes_when_bucket_grants_token() {
        AsyncBucketProxy bucket = mock(AsyncBucketProxy.class);
        ConsumptionProbe probe = mock(ConsumptionProbe.class);
        when(probe.isConsumed()).thenReturn(true);
        when(bucket.tryConsumeAndReturnRemaining(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(probe));

        TokenBucketService svc = new TokenBucketService(bucket, Duration.ofSeconds(5), null);
        assertThat(svc.acquireBlocking().await().indefinitely()).isNull();
    }

    @Test
    void fails_with_throttle_timeout_when_wait_exceeds_limit() {
        AsyncBucketProxy bucket = mock(AsyncBucketProxy.class);
        ConsumptionProbe probe = mock(ConsumptionProbe.class);
        when(probe.isConsumed()).thenReturn(false);
        // reported wait (10 s) >> acquire timeout (100 ms) → immediate timeout
        when(probe.getNanosToWaitForRefill()).thenReturn(Duration.ofSeconds(10).toNanos());
        when(bucket.tryConsumeAndReturnRemaining(eq(1L)))
            .thenReturn(CompletableFuture.completedFuture(probe));

        TokenBucketService svc = new TokenBucketService(bucket, Duration.ofMillis(100), null);
        assertThatThrownBy(() -> svc.acquireBlocking().await().atMost(Duration.ofSeconds(2)))
            .satisfies(t -> {
                if (t instanceof ThrottleTimeoutException) return;
                assertThat(t.getCause()).isInstanceOf(ThrottleTimeoutException.class);
            });
    }
}
