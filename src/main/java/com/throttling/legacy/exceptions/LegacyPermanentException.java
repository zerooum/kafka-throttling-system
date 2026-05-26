package com.throttling.legacy.exceptions;

public class LegacyPermanentException extends RuntimeException {
    private final int status;
    public LegacyPermanentException(int status, String msg) { super(msg); this.status = status; }
    public int status() { return status; }
}
