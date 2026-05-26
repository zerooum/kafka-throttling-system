package com.throttling.throttling;

public class ThrottleTimeoutException extends RuntimeException {
    public ThrottleTimeoutException() {
        super("Token bucket acquire timed out");
    }
}
