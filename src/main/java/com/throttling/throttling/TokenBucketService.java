package com.throttling.throttling;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;

import com.throttling.observability.MetricsRegistry;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.TokensInheritanceStrategy;
import io.github.bucket4j.distributed.AsyncBucketProxy;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TokenBucketService {

    private static final Logger LOG = Logger.getLogger(TokenBucketService.class);

    private final AsyncBucketProxy bucket;
    private final Duration acquireTimeout;
    private final MetricsRegistry metrics;

    @Inject
    public TokenBucketService(
            @Identifier("throttle-redis") RedisClient redisClient,
            BucketConfig config,
            MetricsRegistry metrics) {
        this(buildBucket(redisClient, config), Duration.ofMillis(config.acquireTimeoutMs()), metrics);
    }

    public TokenBucketService(AsyncBucketProxy bucket, Duration acquireTimeout) {
        this(bucket, acquireTimeout, null);
    }

    public TokenBucketService(AsyncBucketProxy bucket, Duration acquireTimeout, MetricsRegistry metrics) {
        this.bucket = bucket;
        this.acquireTimeout = acquireTimeout;
        this.metrics = metrics;
    }

    private static AsyncBucketProxy buildBucket(RedisClient redisClient, BucketConfig cfg) {
        BucketConfiguration bucketCfg = BucketConfiguration.builder()
            .addLimit(Bandwidth.builder()
                .capacity(cfg.capacity())
                .refillGreedy(cfg.refillTokens(), Duration.ofMillis(cfg.refillPeriodMs()))
                .initialTokens(0)
                .build())
            .build();
        LettuceBasedProxyManager<byte[]> proxyManager = LettuceBasedProxyManager
            .builderFor(redisClient)
            .build();
        AsyncBucketProxy bucket = proxyManager.asAsync().builder()
            .build(cfg.bucketKey().getBytes(), bucketCfg);

        // Bucket4j applies the supplied configuration only when the bucket is first
        // created in Redis. A bucket that already exists keeps its STORED config, so
        // edits to throttle.* in application.properties are silently ignored across
        // restarts. Force the current config onto the (possibly pre-existing) bucket.
        try {
            bucket.replaceConfiguration(bucketCfg, TokensInheritanceStrategy.RESET)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
            LOG.infof("Throttle bucket '%s' synced to config: capacity=%d refill=%d/%dms",
                cfg.bucketKey(), cfg.capacity(), cfg.refillTokens(), cfg.refillPeriodMs());
        } catch (Exception e) {
            LOG.warnf(e, "Failed to sync throttle bucket '%s' configuration; "
                + "it may run with a stale stored config", cfg.bucketKey());
        }
        return bucket;
    }

    @WithSpan("throttle.acquire")
    public Uni<Void> acquireBlocking() {
        return tryAcquire(System.nanoTime());
    }

    private Uni<Void> tryAcquire(long startNano) {
        return Uni.createFrom().completionStage(() -> bucket.tryConsumeAndReturnRemaining(1L))
            .onFailure().transform(ThrottleTimeoutException::new)
            .chain(probe -> {
                if (probe.isConsumed()) {
                    if (metrics != null) {
                        metrics.tokenConsumed();
                        metrics.recordWait(Duration.ofNanos(System.nanoTime() - startNano));
                    }
                    return Uni.createFrom().voidItem();
                }
                long waitNanos = probe.getNanosToWaitForRefill();
                if (System.nanoTime() - startNano + waitNanos > acquireTimeout.toNanos()) {
                    if (metrics != null) metrics.throttleTimeout();
                    return Uni.createFrom().failure(new ThrottleTimeoutException());
                }
                return Uni.createFrom().voidItem()
                    .onItem().delayIt().by(Duration.ofNanos(waitNanos))
                    .chain(() -> tryAcquire(startNano));
            });
    }

    public Uni<Long> availableTokens() {
        return Uni.createFrom().completionStage(bucket::getAvailableTokens);
    }
}
