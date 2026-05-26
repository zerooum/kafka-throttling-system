package com.throttling.legacy.exceptions;

public class LegacyTransientException extends RuntimeException {
    private final int status;
    public LegacyTransientException(int status, String msg) { super(msg); this.status = status; }
    public int status() { return status; }
}
