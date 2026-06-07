package com.throttling.processing;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

@ConfigMapping(prefix = "throttle.verify")
public interface VerifyConfig {
    @WithDefault("3") int maxAttempts();
    @WithDefault("1s") Duration baseDelay();
    @WithDefault("2") int backoffMultiplier();
}
