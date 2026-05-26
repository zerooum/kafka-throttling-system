package com.throttling.legacy;

import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.ext.ResponseExceptionMapper;

public class LegacyResponseMapper implements ResponseExceptionMapper<Throwable> {

    @Override
    public Throwable toThrowable(Response response) {
        int s = response.getStatus();
        if (s < 400) return null;
        String body;
        try { body = response.readEntity(String.class); } catch (Exception e) { body = ""; }
        if (s >= 500 || s == 408 || s == 429) {
            return new LegacyTransientException(s, "Legacy " + s + ": " + body);
        }
        return new LegacyPermanentException(s, "Legacy " + s + ": " + body);
    }

    @Override
    public boolean handles(int status, jakarta.ws.rs.core.MultivaluedMap<String, Object> headers) {
        return status >= 400;
    }

    public boolean handles(Response r) {
        return handles(r.getStatus(), null);
    }
}
