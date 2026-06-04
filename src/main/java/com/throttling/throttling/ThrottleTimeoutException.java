package com.throttling.throttling;

public class ThrottleTimeoutException extends RuntimeException {
    public ThrottleTimeoutException() {
        super("Token bucket acquire timed out");
    }

    public ThrottleTimeoutException(Throwable cause) {
        super("Token bucket acquire failed: " + cause.getMessage(), cause);
    }
}
