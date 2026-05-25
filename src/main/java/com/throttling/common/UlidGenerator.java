package com.throttling.common;

import com.github.f4b6a3.ulid.UlidCreator;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class UlidGenerator {
    public String next() {
        return UlidCreator.getUlid().toString();
    }
}
