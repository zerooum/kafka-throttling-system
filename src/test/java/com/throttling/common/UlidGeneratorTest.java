package com.throttling.common;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class UlidGeneratorTest {

    @Test
    void generates_ulid_with_26_chars() {
        UlidGenerator g = new UlidGenerator();
        String id = g.next();
        assertThat(id).hasSize(26).matches("[0-9A-HJKMNP-TV-Z]{26}");
    }

    @Test
    void generates_unique_values() {
        UlidGenerator g = new UlidGenerator();
        assertThat(g.next()).isNotEqualTo(g.next());
    }
}
