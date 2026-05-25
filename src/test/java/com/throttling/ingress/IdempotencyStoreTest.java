package com.throttling.ingress;

import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.SetArgs;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdempotencyStoreTest {

    @Test
    void try_store_returns_empty_when_set_succeeds() {
        ReactiveRedisDataSource ds = mock(ReactiveRedisDataSource.class);
        ReactiveValueCommands<String, String> values = mock(ReactiveValueCommands.class);
        when(ds.value(String.class)).thenReturn(values);
        when(values.setGet(eq("idemp:k1"), eq("M1"), any(SetArgs.class)))
            .thenReturn(Uni.createFrom().nullItem());

        IdempotencyStore store = new IdempotencyStore(ds);

        Optional<String> result = store
            .tryStore("k1", "M1", Duration.ofSeconds(60))
            .await().indefinitely();

        assertThat(result).isEmpty();
    }

    @Test
    void try_store_returns_existing_when_key_present() {
        ReactiveRedisDataSource ds = mock(ReactiveRedisDataSource.class);
        ReactiveValueCommands<String, String> values = mock(ReactiveValueCommands.class);
        when(ds.value(String.class)).thenReturn(values);
        when(values.setGet(eq("idemp:k1"), eq("M2"), any(SetArgs.class)))
            .thenReturn(Uni.createFrom().item("M1"));

        IdempotencyStore store = new IdempotencyStore(ds);

        Optional<String> result = store
            .tryStore("k1", "M2", Duration.ofSeconds(60))
            .await().indefinitely();

        assertThat(result).contains("M1");
    }

    @Test
    void remove_calls_redis_del() {
        ReactiveRedisDataSource ds = mock(ReactiveRedisDataSource.class);
        io.quarkus.redis.datasource.keys.ReactiveKeyCommands<String> keys =
            mock(io.quarkus.redis.datasource.keys.ReactiveKeyCommands.class);
        when(ds.key()).thenReturn(keys);
        when(keys.del("idemp:k1")).thenReturn(Uni.createFrom().item(1));

        IdempotencyStore store = new IdempotencyStore(ds);
        store.remove("k1").await().indefinitely();

        verify(keys).del("idemp:k1");
    }
}
