package com.throttling.ingress;

import com.throttling.ingress.dto.IngressRequest;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@QuarkusTest
class MessagesResourceTest {

    @InjectMock IngressService service;

    @Test
    void returns_202_with_message_id_when_accepted() {
        when(service.handle(eq("k1"), any(), any(IngressRequest.class)))
            .thenReturn(Uni.createFrom().item(
                new IngressOutcome.Accepted("M1", Instant.parse("2026-05-25T14:30:00Z"))
            ));

        given()
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", "k1")
            .body(Map.of("payload", Map.of("a", 1)))
        .when()
            .post("/messages")
        .then()
            .statusCode(202)
            .body("messageId", equalTo("M1"))
            .body("acceptedAt", notNullValue());
    }

    @Test
    void returns_409_when_duplicate() {
        when(service.handle(eq("k1"), any(), any(IngressRequest.class)))
            .thenReturn(Uni.createFrom().item(new IngressOutcome.Duplicate("M_ORIG")));

        given()
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", "k1")
            .body(Map.of("payload", Map.of("a", 1)))
        .when()
            .post("/messages")
        .then()
            .statusCode(409)
            .body("error", equalTo("DUPLICATE"))
            .body("messageId", equalTo("M_ORIG"))
            .body("idempotencyKey", equalTo("k1"));
    }

    @Test
    void returns_400_when_idempotency_header_missing() {
        given()
            .contentType(MediaType.APPLICATION_JSON)
            .body(Map.of("payload", Map.of("a", 1)))
        .when()
            .post("/messages")
        .then()
            .statusCode(400);
    }
}
