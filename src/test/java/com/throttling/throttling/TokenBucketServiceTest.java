package com.throttling.throttling;

import io.github.bucket4j.SchedulingBucket;
import io.github.bucket4j.distributed.AsyncBucketProxy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TokenBucketServiceTest {

    @Test
    void completes_when_bucket_grants_token() {
        AsyncBucketProxy bucket = mock(AsyncBucketProxy.class);
        SchedulingBucket sched = mock(SchedulingBucket.class);
        when(bucket.asScheduler()).thenReturn(sched);
        when(sched.consume(eq(1L), any(ScheduledExecutorService.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        try {
            TokenBucketService svc = new TokenBucketService(bucket, Duration.ofSeconds(5), exec);
            assertThat(svc.acquireBlocking().await().indefinitely()).isNull();
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    void fails_with_throttle_timeout_when_acquire_exceeds_limit() {
        AsyncBucketProxy bucket = mock(AsyncBucketProxy.class);
        SchedulingBucket sched = mock(SchedulingBucket.class);
        when(bucket.asScheduler()).thenReturn(sched);
        CompletableFuture<Void> never = new CompletableFuture<>();
        when(sched.consume(eq(1L), any(ScheduledExecutorService.class))).thenReturn(never);

        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        try {
            TokenBucketService svc = new TokenBucketService(bucket, Duration.ofMillis(100), exec);
            assertThatThrownBy(() -> svc.acquireBlocking().await().atMost(Duration.ofSeconds(2)))
                .satisfies(t -> {
                    if (t instanceof ThrottleTimeoutException) return;
                    assertThat(t.getCause()).isInstanceOf(ThrottleTimeoutException.class);
                });
        } finally {
            exec.shutdownNow();
        }
    }
}
