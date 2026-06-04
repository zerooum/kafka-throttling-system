package com.throttling.legacy;

import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LegacyResponseMapperTest {

    LegacyResponseMapper mapper = new LegacyResponseMapper();

    private Response responseWithStatus(int code) {
        Response r = mock(Response.class);
        when(r.getStatus()).thenReturn(code);
        when(r.readEntity(String.class)).thenReturn("body-" + code);
        return r;
    }

    @Test
    void handles_5xx_as_transient() {
        Throwable t = mapper.toThrowable(responseWithStatus(503));
        assertThat(t).isInstanceOf(LegacyTransientException.class);
    }

    @Test
    void handles_408_as_transient() {
        Throwable t = mapper.toThrowable(responseWithStatus(408));
        assertThat(t).isInstanceOf(LegacyTransientException.class);
    }

    @Test
    void handles_429_as_transient() {
        Throwable t = mapper.toThrowable(responseWithStatus(429));
        assertThat(t).isInstanceOf(LegacyTransientException.class);
    }

    @Test
    void handles_other_4xx_as_permanent() {
        Throwable t = mapper.toThrowable(responseWithStatus(400));
        assertThat(t).isInstanceOf(LegacyPermanentException.class);
    }

    @Test
    void returns_null_for_2xx() {
        assertThat(mapper.toThrowable(responseWithStatus(200))).isNull();
    }

    @Test
    void handles_value_when_status_4xx_or_5xx() {
        assertThat(mapper.handles(responseWithStatus(500))).isTrue();
        assertThat(mapper.handles(responseWithStatus(404))).isTrue();
        assertThat(mapper.handles(responseWithStatus(200))).isFalse();
    }
}
