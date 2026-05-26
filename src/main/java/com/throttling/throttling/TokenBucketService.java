package com.throttling.throttling;

import com.throttling.observability.MetricsRegistry;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.AsyncBucketProxy;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@ApplicationScoped
public class TokenBucketService {

    private final AsyncBucketProxy bucket;
    private final Duration acquireTimeout;
    private final ScheduledExecutorService scheduler;
    private final MetricsRegistry metrics;

    @Inject
    public TokenBucketService(
            @Identifier("throttle-redis") RedisClient redisClient,
            BucketConfig config,
            MetricsRegistry metrics) {
        this(buildBucket(redisClient, config), Duration.ofMillis(config.acquireTimeoutMs()), metrics);
    }

    public TokenBucketService(AsyncBucketProxy bucket, Duration acquireTimeout) {
        this(bucket, acquireTimeout, Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "throttle-scheduler");
            t.setDaemon(true);
            return t;
        }), null);
    }

    public TokenBucketService(AsyncBucketProxy bucket, Duration acquireTimeout, MetricsRegistry metrics) {
        this(bucket, acquireTimeout, Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "throttle-scheduler");
            t.setDaemon(true);
            return t;
        }), metrics);
    }

    public TokenBucketService(AsyncBucketProxy bucket, Duration acquireTimeout,
                              ScheduledExecutorService scheduler, MetricsRegistry metrics) {
        this.bucket = bucket;
        this.acquireTimeout = acquireTimeout;
        this.scheduler = scheduler;
        this.metrics = metrics;
    }

    private static AsyncBucketProxy buildBucket(RedisClient redisClient, BucketConfig cfg) {
        BucketConfiguration bucketCfg = BucketConfiguration.builder()
            .addLimit(Bandwidth.builder()
                .capacity(cfg.capacity())
                .refillGreedy(cfg.refillTokens(), Duration.ofMillis(cfg.refillPeriodMs()))
                .build())
            .build();
        LettuceBasedProxyManager<byte[]> proxyManager = LettuceBasedProxyManager
            .builderFor(redisClient)
            .build();
        return proxyManager.asAsync().builder()
            .build(cfg.bucketKey().getBytes(), bucketCfg);
    }

    public Uni<Void> acquireBlocking() {
        long start = System.nanoTime();
        return Uni.createFrom().completionStage(bucket.asScheduler().consume(1L, scheduler))
            .ifNoItem().after(acquireTimeout).failWith(new ThrottleTimeoutException())
            .invoke(() -> {
                if (metrics != null) {
                    metrics.tokenConsumed();
                    metrics.recordWait(Duration.ofNanos(System.nanoTime() - start));
                }
            })
            .onFailure(ThrottleTimeoutException.class).invoke(() -> { if (metrics != null) metrics.throttleTimeout(); })
            .replaceWithVoid();
    }

    public Uni<Long> availableTokens() {
        return Uni.createFrom().completionStage(bucket.getAvailableTokens());
    }
}
