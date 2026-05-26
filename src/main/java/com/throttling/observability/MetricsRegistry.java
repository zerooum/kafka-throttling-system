package com.throttling.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;

@ApplicationScoped
public class MetricsRegistry {

    private final MeterRegistry registry;

    @Inject
    public MetricsRegistry(MeterRegistry registry) {
        this.registry = registry;
    }

    public void ingressAccepted() { counter("messages.ingress.received", "outcome", "accepted").increment(); }
    public void ingressDuplicate() {
        counter("messages.ingress.received", "outcome", "duplicate").increment();
        counter("messages.ingress.idempotency.duplicate").increment();
    }
    public void ingressRejected() { counter("messages.ingress.received", "outcome", "rejected").increment(); }

    public void consumed(String outcome) { counter("messages.consumed", "outcome", outcome).increment(); }
    public void tokenConsumed() { counter("throttle.tokens.consumed").increment(); }
    public void throttleTimeout() { counter("throttle.acquire.timeout").increment(); }
    public void recordWait(Duration d) { timer("throttle.tokens.wait.duration").record(d); }

    public void dlqSent(String reason) { counter("dlq.messages.sent", "reason", reason).increment(); }

    private Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }
    private Timer timer(String name) {
        return Timer.builder(name).publishPercentiles(0.5, 0.95, 0.99).register(registry);
    }
}
