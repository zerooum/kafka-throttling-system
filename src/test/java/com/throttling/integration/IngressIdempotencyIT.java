package com.throttling.integration;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(RedisTestResource.class)
@QuarkusTestResource(WiremockTestResource.class)
class IngressIdempotencyIT {

    @InjectKafkaCompanion KafkaCompanion companion;

    @Test
    void second_post_with_same_idempotency_key_returns_409_and_only_one_record_on_topic() {
        String key = "it-key-" + System.nanoTime();
        Map<String, Object> body = Map.of("payload", Map.of("x", 1));

        String messageId = given()
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", key)
            .body(body)
        .when()
            .post("/messages")
        .then()
            .statusCode(202)
            .body("messageId", notNullValue())
            .extract().path("messageId");

        given()
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", key)
            .body(body)
        .when()
            .post("/messages")
        .then()
            .statusCode(409)
            .body("error", equalTo("DUPLICATE"))
            .body("messageId", equalTo(messageId));

        ConsumerTask<String, String> records = companion.consumeStrings()
            .fromTopics("messages.in", 1, Duration.ofSeconds(10));
        assertThat(records.count()).isEqualTo(1);
    }
}
