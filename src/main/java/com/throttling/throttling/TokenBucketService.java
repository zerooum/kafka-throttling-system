package com.throttling.throttling;

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

    @Inject
    public TokenBucketService(
            @Identifier("throttle-redis") RedisClient redisClient,
            BucketConfig config) {
        this(buildBucket(redisClient, config), Duration.ofMillis(config.acquireTimeoutMs()));
    }

    public TokenBucketService(AsyncBucketProxy bucket, Duration acquireTimeout) {
        this(bucket, acquireTimeout, Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "throttle-scheduler");
            t.setDaemon(true);
            return t;
        }));
    }

    public TokenBucketService(AsyncBucketProxy bucket, Duration acquireTimeout, ScheduledExecutorService scheduler) {
        this.bucket = bucket;
        this.acquireTimeout = acquireTimeout;
        this.scheduler = scheduler;
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
        return Uni.createFrom().completionStage(bucket.asScheduler().consume(1L, scheduler))
            .ifNoItem().after(acquireTimeout).failWith(new ThrottleTimeoutException())
            .replaceWithVoid();
    }

    public Uni<Long> availableTokens() {
        return Uni.createFrom().completionStage(bucket.getAvailableTokens());
    }
}
