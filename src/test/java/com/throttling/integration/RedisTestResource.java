package com.throttling.integration;

import com.redis.testcontainers.RedisContainer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class RedisTestResource implements QuarkusTestResourceLifecycleManager {

    private RedisContainer redis;

    @Override
    public Map<String, String> start() {
        redis = new RedisContainer(DockerImageName.parse("redis:7.4-alpine"));
        redis.start();
        String host = redis.getHost();
        Integer port = redis.getFirstMappedPort();
        return Map.of(
            "quarkus.redis.hosts", "redis://" + host + ":" + port,
            "throttle.redis.host", host,
            "throttle.redis.port", String.valueOf(port)
        );
    }

    @Override
    public void stop() {
        if (redis != null) redis.stop();
    }
}
