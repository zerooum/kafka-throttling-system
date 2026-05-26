package com.throttling.throttling;

import io.github.bucket4j.distributed.AsyncBucketProxy;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class TokenBucketServiceTest {

    @Test
    void completes_when_bucket_grants_token() {
        AsyncBucketProxy bucket = Mockito.mock(AsyncBucketProxy.class);
        when(bucket.tryConsume(1L)).thenReturn(CompletableFuture.completedFuture(true));

        TokenBucketService svc = new TokenBucketService(bucket, Duration.ofSeconds(5));
        assertThat(svc.acquireBlocking().await().indefinitely()).isNull();
    }

    @Test
    void fails_with_throttle_timeout_when_acquire_exceeds_limit() {
        AsyncBucketProxy bucket = Mockito.mock(AsyncBucketProxy.class);
        CompletableFuture<Boolean> never = new CompletableFuture<>();
        when(bucket.tryConsume(1L)).thenReturn(never);

        TokenBucketService svc = new TokenBucketService(bucket, Duration.ofMillis(100));
        assertThatThrownBy(() -> svc.acquireBlocking().await().atMost(Duration.ofSeconds(2)))
            .satisfies(t -> {
                boolean isDirectly = t instanceof ThrottleTimeoutException;
                boolean isWrapped = t.getCause() instanceof ThrottleTimeoutException;
                org.assertj.core.api.Assertions.assertThat(isDirectly || isWrapped)
                    .as("Expected ThrottleTimeoutException directly or as cause")
                    .isTrue();
            });
    }
}
