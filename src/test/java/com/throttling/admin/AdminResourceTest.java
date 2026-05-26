package com.throttling.admin;

import com.throttling.throttling.BucketConfig;
import com.throttling.throttling.TokenBucketService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.faulttolerance.api.CircuitBreakerMaintenance;
import io.smallrye.faulttolerance.api.CircuitBreakerState;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@QuarkusTest
class AdminResourceTest {

    @InjectMock TokenBucketService bucket;
    @InjectMock BucketConfig cfg;
    @InjectMock CircuitBreakerMaintenance cb;

    @Test
    void admin_throttle_returns_state() {
        when(bucket.availableTokens()).thenReturn(Uni.createFrom().item(42L));
        when(cfg.capacity()).thenReturn(100L);
        when(cfg.refillTokens()).thenReturn(50L);
        when(cfg.refillPeriodMs()).thenReturn(1000L);

        given().header("X-Admin-Token", "dev-admin")
            .when().get("/admin/throttle")
            .then().statusCode(200)
            .body("capacity", equalTo(100))
            .body("available", equalTo(42));
    }

    @Test
    void admin_endpoints_return_401_without_token() {
        given().when().get("/admin/throttle").then().statusCode(401);
    }

    @Test
    void admin_circuit_breaker_status_returns_state() {
        when(cb.currentState("legacy-client")).thenReturn(CircuitBreakerState.CLOSED);
        given().header("X-Admin-Token", "dev-admin")
            .when().get("/admin/circuit-breaker/status")
            .then().statusCode(200)
            .body("state", equalTo("CLOSED"));
    }
}
