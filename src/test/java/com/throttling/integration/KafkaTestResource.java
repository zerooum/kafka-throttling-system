package com.throttling.integration;

import java.util.Map;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

public class KafkaTestResource implements QuarkusTestResourceLifecycleManager {

    private KafkaContainer kafka;
    private KafkaCompanion companion;

    @Override
    public Map<String, String> start() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        kafka.start();
        String bootstrapServers = kafka.getBootstrapServers();
        companion = new KafkaCompanion(bootstrapServers);
        return Map.of("kafka.bootstrap.servers", bootstrapServers);
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(companion,
            new TestInjector.AnnotatedAndMatchesType(InjectKafkaCompanion.class, KafkaCompanion.class));
    }

    @Override
    public void stop() {
        if (companion != null) companion.close();
        if (kafka != null) kafka.stop();
    }
}
