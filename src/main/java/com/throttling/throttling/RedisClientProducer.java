package com.throttling.throttling;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RedisClientProducer {

    @Produces
    @ApplicationScoped
    @Identifier("throttle-redis")
    public RedisClient redisClient(
            @ConfigProperty(name = "throttle.redis.host", defaultValue = "localhost") String host,
            @ConfigProperty(name = "throttle.redis.port", defaultValue = "6379") int port) {
        return RedisClient.create(RedisURI.builder().withHost(host).withPort(port).build());
    }

    public void close(@Disposes @Identifier("throttle-redis") RedisClient client) {
        client.shutdown();
    }
}
