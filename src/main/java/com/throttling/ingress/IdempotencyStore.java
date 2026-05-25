package com.throttling.ingress;

import com.throttling.common.Constants;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.SetArgs;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.Optional;

@ApplicationScoped
public class IdempotencyStore {

    private final ReactiveRedisDataSource ds;

    @Inject
    public IdempotencyStore(ReactiveRedisDataSource ds) {
        this.ds = ds;
    }

    public Uni<Optional<String>> tryStore(String idempotencyKey, String messageId, Duration ttl) {
        String redisKey = Constants.IDEMP_KEY_PREFIX + idempotencyKey;
        SetArgs args = new SetArgs().nx().ex(ttl.toSeconds());
        return ds.value(String.class)
            .setGet(redisKey, messageId, args)
            .map(Optional::ofNullable);
    }

    public Uni<Void> remove(String idempotencyKey) {
        String redisKey = Constants.IDEMP_KEY_PREFIX + idempotencyKey;
        return ds.key().del(redisKey).replaceWithVoid();
    }
}
