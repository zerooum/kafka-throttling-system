package com.throttling.throttling;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "throttle")
public interface BucketConfig {
    long capacity();
    long refillTokens();
    long refillPeriodMs();
    String bucketKey();
    long acquireTimeoutMs();

    Redis redis();

    interface Redis {
        @io.smallrye.config.WithDefault("localhost") String host();
        @io.smallrye.config.WithDefault("6379") int port();
    }
}
