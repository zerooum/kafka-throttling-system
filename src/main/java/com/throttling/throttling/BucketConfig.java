package com.throttling.throttling;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "throttle")
public interface BucketConfig {
    long capacity();
    long refillTokens();
    long refillPeriodMs();
    String bucketKey();
    long acquireTimeoutMs();
}
