package com.throttling.ingress;

import com.throttling.common.Constants;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.Optional;

/**
 * Exemplo: idempotency store usando o mesmo cliente Lettuce do throttling,
 * eliminando o cliente Quarkus Redis dedicado.
 *
 * Reusa o {@code @Identifier("throttle-redis") RedisClient} produzido em
 * {@link com.throttling.throttling.RedisClientProducer}. Para ativar, anote
 * com {@code @ApplicationScoped} e remova a {@link IdempotencyStore} atual
 * (mais a dependência quarkus-redis-client e a propriedade quarkus.redis.hosts).
 */
public class LettuceIdempotencyStore {

    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> commands;

    @Inject
    public LettuceIdempotencyStore(@Identifier("throttle-redis") RedisClient client) {
        this.connection = client.connect();           // conexão multiplexada, thread-safe
        this.commands = connection.async();
    }

    public Uni<Optional<String>> tryStore(String idempotencyKey, String messageId, Duration ttl) {
        String redisKey = Constants.IDEMP_KEY_PREFIX + idempotencyKey;
        SetArgs args = SetArgs.Builder.nx().ex(ttl.toSeconds());
        // SET key val NX EX ttl GET -> valor antigo se já existia, null se setou (exige Redis 7.0+)
        return Uni.createFrom()
            .completionStage(() -> commands.setGet(redisKey, messageId, args))
            .map(Optional::ofNullable);
    }

    public Uni<Void> remove(String idempotencyKey) {
        String redisKey = Constants.IDEMP_KEY_PREFIX + idempotencyKey;
        return Uni.createFrom()
            .completionStage(() -> commands.del(redisKey))
            .replaceWithVoid();
    }

    @PreDestroy
    void close() {
        connection.close();
    }
}
