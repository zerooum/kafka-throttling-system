package com.throttling.integration;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class PostgresTestResource implements QuarkusTestResourceLifecycleManager {

    private PostgreSQLContainer<?> pg;

    @Override
    public Map<String, String> start() {
        pg = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("throttling")
            .withUsername("throttling")
            .withPassword("throttling");
        pg.start();
        String reactiveUrl = "postgresql://" + pg.getHost() + ":" + pg.getFirstMappedPort() + "/throttling";
        return Map.of(
            "quarkus.datasource.db-kind", "postgresql",
            "quarkus.datasource.username", "throttling",
            "quarkus.datasource.password", "throttling",
            "quarkus.datasource.reactive.url", reactiveUrl
        );
    }

    @Override
    public void stop() {
        if (pg != null) pg.stop();
    }
}
