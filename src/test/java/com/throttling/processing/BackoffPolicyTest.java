package com.throttling.processing;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffPolicyTest {

    @Test
    void grows_geometrically_from_base() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofSeconds(1), 2);

        assertThat(policy.delayForAttempt(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.delayForAttempt(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayForAttempt(3)).isEqualTo(Duration.ofSeconds(4));
    }

    @Test
    void respects_custom_base_and_multiplier() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofMillis(500), 3);

        assertThat(policy.delayForAttempt(1)).isEqualTo(Duration.ofMillis(500));
        assertThat(policy.delayForAttempt(2)).isEqualTo(Duration.ofMillis(1500));
        assertThat(policy.delayForAttempt(3)).isEqualTo(Duration.ofMillis(4500));
    }
}
