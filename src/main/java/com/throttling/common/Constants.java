package com.throttling.common;

public final class Constants {
    private Constants() {}

    public static final String IDEMP_KEY_PREFIX = "idemp:";
    public static final String HEADER_IDEMP_KEY = "X-Idempotency-Key";
    public static final String HEADER_TRACEPARENT = "traceparent";
    public static final String HEADER_TRACESTATE = "tracestate";
    public static final String HEADER_MESSAGE_ID = "messageId";

    public static final String TOPIC_MESSAGES_IN = "messages.in";
    public static final String TOPIC_MESSAGES_DLQ = "messages.dlq";
}
