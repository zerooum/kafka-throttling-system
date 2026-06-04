package com.throttling.throttling;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class RedisClientProducer {

    @Produces
    @ApplicationScoped
    @Identifier("throttle-redis")
    public RedisClient redisClient(BucketConfig cfg) {
        return RedisClient.create(RedisURI.builder()
            .withHost(cfg.redis().host())
            .withPort(cfg.redis().port())
            .build());
    }

    public void close(@Disposes @Identifier("throttle-redis") RedisClient client) {
        client.shutdown();
    }
}
