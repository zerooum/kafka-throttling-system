# Verify-in-Table and Retry on External API Timeout — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When the legacy external API errors with a timeout or transient 5xx, wait (growing backoff), check a reactive Postgres table for the `idempotencyKey`; if present the legado processed it (ack), if absent retry — up to 3 API calls total, then DLQ.

**Architecture:** A new `RetryOrchestrator` (`@ApplicationScoped`) drives the call → wait → verify → retry loop reactively with Mutiny. It reads a `processing_record` table through a `VerificationStore` interface (Panache reactive impl). `BackoffPolicy` computes per-attempt delays. `MessageHandler` delegates to the orchestrator; its DLQ classification path is unchanged. The legacy REST client's blind `@Retry` is removed so retries only happen after the table check.

**Tech Stack:** Quarkus 3.17.4, Mutiny, Hibernate Reactive Panache + reactive PG client, SmallRye Fault Tolerance, Micrometer, JUnit 5 + Mockito + AssertJ + Testcontainers.

---

## File Structure

**New (main):**
- `src/main/java/com/throttling/verification/ProcessingRecord.java` — Panache reactive entity, table `processing_record`.
- `src/main/java/com/throttling/verification/VerificationStore.java` — interface `exists(idempotencyKey): Uni<Boolean>`.
- `src/main/java/com/throttling/verification/PgVerificationStore.java` — Panache impl.
- `src/main/java/com/throttling/processing/VerifyConfig.java` — `@ConfigMapping` for `throttle.verify.*`.
- `src/main/java/com/throttling/processing/BackoffPolicy.java` — per-attempt delay.
- `src/main/java/com/throttling/processing/RetryOrchestrator.java` — the reactive loop.

**New (test):**
- `src/test/java/com/throttling/processing/BackoffPolicyTest.java`
- `src/test/java/com/throttling/processing/RetryOrchestratorTest.java`
- `src/test/java/com/throttling/verification/PgVerificationStoreTest.java`
- `src/test/java/com/throttling/integration/PostgresTestResource.java`

**Modified:**
- `pom.xml` — add reactive Panache, reactive PG, testcontainers-postgres, quarkus-test-vertx.
- `docker-compose.yml` — add `postgres` service.
- `src/main/resources/application.properties` — datasource + `throttle.verify.*` config.
- `src/main/java/com/throttling/legacy/LegacyClient.java` — remove `@Retry`.
- `src/main/java/com/throttling/processing/MessageHandler.java` — delegate to orchestrator.
- `src/test/java/com/throttling/processing/MessageHandlerTest.java` — mock orchestrator.
- `src/main/java/com/throttling/observability/MetricsRegistry.java` — `verifyChecked`, `apiRetried`.
- `src/main/java/com/throttling/legacy/MockLegacyResource.java` — write row + `simulateTimeout`.

---

## Task 1: Infra — dependencies, Postgres service, config

**Files:**
- Modify: `pom.xml`
- Modify: `docker-compose.yml`
- Modify: `src/main/resources/application.properties`

- [ ] **Step 1: Add main dependencies to `pom.xml`**

Insert after the `quarkus-redis-client` line (around `pom.xml:43`):

```xml
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-hibernate-reactive-panache</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-reactive-pg-client</artifactId></dependency>
```

- [ ] **Step 2: Add test dependencies to `pom.xml`**

Insert in the `<!-- test -->` block, after the `testcontainers-redis` dependency (around `pom.xml:98`):

```xml
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-test-vertx</artifactId><scope>test</scope></dependency>
```

- [ ] **Step 3: Add the `postgres` service to `docker-compose.yml`**

Insert after the `redis` service block (after `docker-compose.yml:26`):

```yaml
  postgres:
    image: postgres:16-alpine
    container_name: kts-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: throttling
      POSTGRES_PASSWORD: throttling
      POSTGRES_DB: throttling
```

- [ ] **Step 4: Add datasource + verify config to `application.properties`**

Append at the end of `src/main/resources/application.properties`:

```properties
# Verification DB (Postgres reactive)
quarkus.datasource.db-kind=postgresql
quarkus.datasource.username=throttling
quarkus.datasource.password=throttling
quarkus.datasource.reactive.url=postgresql://localhost:5432/throttling
quarkus.hibernate-orm.database.generation=none
%dev.quarkus.hibernate-orm.database.generation=drop-and-create
%test.quarkus.hibernate-orm.database.generation=drop-and-create

# Verify + retry cycle
throttle.verify.max-attempts=3
throttle.verify.base-delay=1s
throttle.verify.backoff-multiplier=2
```

- [ ] **Step 5: Verify the project still compiles**

Run: `./mvnw -q compile`
Expected: BUILD SUCCESS (new deps resolve; no code uses them yet).

- [ ] **Step 6: Commit**

```bash
git add pom.xml docker-compose.yml src/main/resources/application.properties
git commit -m "build: add reactive Postgres datasource and verify config"
```

---

## Task 2: BackoffPolicy + config

**Files:**
- Create: `src/main/java/com/throttling/processing/VerifyConfig.java`
- Create: `src/main/java/com/throttling/processing/BackoffPolicy.java`
- Test: `src/test/java/com/throttling/processing/BackoffPolicyTest.java`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/throttling/processing/BackoffPolicyTest.java`:

```java
package com.throttling.processing;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffPolicyTest {

    @Test
    void grows_geometrically_from_base() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofSeconds(1), 2);

        assertThat(policy.delayForAttempt(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.delayForAttempt(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayForAttempt(3)).isEqualTo(Duration.ofSeconds(4));
    }

    @Test
    void respects_custom_base_and_multiplier() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofMillis(500), 3);

        assertThat(policy.delayForAttempt(1)).isEqualTo(Duration.ofMillis(500));
        assertThat(policy.delayForAttempt(2)).isEqualTo(Duration.ofMillis(1500));
        assertThat(policy.delayForAttempt(3)).isEqualTo(Duration.ofMillis(4500));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -q test -Dtest=BackoffPolicyTest`
Expected: FAIL — `BackoffPolicy` / `VerifyConfig` do not exist (compilation error).

- [ ] **Step 3: Create `VerifyConfig`**

Create `src/main/java/com/throttling/processing/VerifyConfig.java`:

```java
package com.throttling.processing;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

@ConfigMapping(prefix = "throttle.verify")
public interface VerifyConfig {
    @WithDefault("3") int maxAttempts();
    @WithDefault("1s") Duration baseDelay();
    @WithDefault("2") int backoffMultiplier();
}
```

- [ ] **Step 4: Create `BackoffPolicy`**

Create `src/main/java/com/throttling/processing/BackoffPolicy.java`:

```java
package com.throttling.processing;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;

@ApplicationScoped
public class BackoffPolicy {

    private final Duration base;
    private final int multiplier;

    @Inject
    public BackoffPolicy(VerifyConfig config) {
        this(config.baseDelay(), config.backoffMultiplier());
    }

    public BackoffPolicy(Duration base, int multiplier) {
        this.base = base;
        this.multiplier = multiplier;
    }

    /** Delay before the verification check that follows attempt {@code n} (1-based). */
    public Duration delayForAttempt(int n) {
        long factor = (long) Math.pow(multiplier, n - 1);
        return base.multipliedBy(factor);
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./mvnw -q test -Dtest=BackoffPolicyTest`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/throttling/processing/VerifyConfig.java \
        src/main/java/com/throttling/processing/BackoffPolicy.java \
        src/test/java/com/throttling/processing/BackoffPolicyTest.java
git commit -m "feat(verify): add BackoffPolicy with geometric backoff config"
```

---

## Task 3: ProcessingRecord entity + VerificationStore (Panache reactive)

**Files:**
- Create: `src/main/java/com/throttling/verification/ProcessingRecord.java`
- Create: `src/main/java/com/throttling/verification/VerificationStore.java`
- Create: `src/main/java/com/throttling/verification/PgVerificationStore.java`
- Create: `src/test/java/com/throttling/integration/PostgresTestResource.java`
- Test: `src/test/java/com/throttling/verification/PgVerificationStoreTest.java`

- [ ] **Step 1: Create the entity**

Create `src/main/java/com/throttling/verification/ProcessingRecord.java`:

```java
package com.throttling.verification;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "processing_record")
public class ProcessingRecord extends PanacheEntityBase {

    @Id
    @Column(name = "idempotency_key")
    public String idempotencyKey;

    @Column(name = "processed_at")
    public Instant processedAt;
}
```

- [ ] **Step 2: Create the `VerificationStore` interface**

Create `src/main/java/com/throttling/verification/VerificationStore.java`:

```java
package com.throttling.verification;

import io.smallrye.mutiny.Uni;

public interface VerificationStore {
    /** True when the legado recorded processing for this idempotency key. */
    Uni<Boolean> exists(String idempotencyKey);
}
```

- [ ] **Step 3: Create the Panache implementation**

Create `src/main/java/com/throttling/verification/PgVerificationStore.java`:

```java
package com.throttling.verification;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PgVerificationStore implements VerificationStore {

    @Override
    public Uni<Boolean> exists(String idempotencyKey) {
        return Panache.withSession(() ->
                ProcessingRecord.count("idempotencyKey", idempotencyKey))
            .map(count -> count > 0);
    }
}
```

- [ ] **Step 4: Create the Postgres test resource**

Create `src/test/java/com/throttling/integration/PostgresTestResource.java`:

```java
package com.throttling.integration;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class PostgresTestResource implements QuarkusTestResourceLifecycleManager {

    private PostgreSQLContainer<?> pg;

    @Override
    public Map<String, String> start() {
        pg = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("throttling")
            .withUsername("throttling")
            .withPassword("throttling");
        pg.start();
        String reactiveUrl = "postgresql://" + pg.getHost() + ":" + pg.getFirstMappedPort() + "/throttling";
        return Map.of(
            "quarkus.datasource.db-kind", "postgresql",
            "quarkus.datasource.username", "throttling",
            "quarkus.datasource.password", "throttling",
            "quarkus.datasource.reactive.url", reactiveUrl,
            "quarkus.hibernate-orm.database.generation", "drop-and-create"
        );
    }

    @Override
    public void stop() {
        if (pg != null) pg.stop();
    }
}
```

- [ ] **Step 5: Write the failing repository test**

Create `src/test/java/com/throttling/verification/PgVerificationStoreTest.java`:

```java
package com.throttling.verification;

import com.throttling.integration.PostgresTestResource;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class PgVerificationStoreTest {

    @Inject
    VerificationStore store;

    @Test
    @RunOnVertxContext
    void exists_is_false_when_absent(UniAsserter asserter) {
        asserter.assertThat(() -> store.exists("missing-key"),
            found -> assertThat(found).isFalse());
    }

    @Test
    @RunOnVertxContext
    void exists_is_true_after_record_written(UniAsserter asserter) {
        asserter.execute(() -> Panache.withTransaction(() -> {
            ProcessingRecord rec = new ProcessingRecord();
            rec.idempotencyKey = "present-key";
            rec.processedAt = Instant.now();
            return rec.persist();
        }));
        asserter.assertThat(() -> store.exists("present-key"),
            found -> assertThat(found).isTrue());
    }
}
```

- [ ] **Step 6: Run test to verify it passes (Docker required for Testcontainers)**

Run: `./mvnw -q test -Dtest=PgVerificationStoreTest`
Expected: PASS (2 tests). On first run Testcontainers pulls `postgres:16-alpine`.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/throttling/verification/ \
        src/test/java/com/throttling/verification/PgVerificationStoreTest.java \
        src/test/java/com/throttling/integration/PostgresTestResource.java
git commit -m "feat(verify): add ProcessingRecord entity and reactive VerificationStore"
```

---

## Task 4: Metrics for verify + retry

**Files:**
- Modify: `src/main/java/com/throttling/observability/MetricsRegistry.java`

- [ ] **Step 1: Add the two counter methods**

In `src/main/java/com/throttling/observability/MetricsRegistry.java`, insert after the `dlqSent` method (after line 33):

```java
    public void verifyChecked(String result) { counter("legacy.verify.checked", "result", result).increment(); }
    public void apiRetried() { counter("legacy.api.retried").increment(); }
```

- [ ] **Step 2: Verify it compiles**

Run: `./mvnw -q compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/throttling/observability/MetricsRegistry.java
git commit -m "feat(observability): add verify-checked and api-retried counters"
```

---

## Task 5: RetryOrchestrator

**Files:**
- Create: `src/main/java/com/throttling/processing/RetryOrchestrator.java`
- Test: `src/test/java/com/throttling/processing/RetryOrchestratorTest.java`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/throttling/processing/RetryOrchestratorTest.java`:

```java
package com.throttling.processing;

import com.throttling.common.MessageEnvelope;
import com.throttling.legacy.LegacyClient;
import com.throttling.legacy.LegacyResponse;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.TokenBucketService;
import com.throttling.verification.VerificationStore;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RetryOrchestratorTest {

    TokenBucketService throttle;
    LegacyClient legacy;
    VerificationStore verify;
    BackoffPolicy backoff;
    MetricsRegistry metrics;
    RetryOrchestrator orchestrator;

    @BeforeEach
    void setup() {
        throttle = mock(TokenBucketService.class);
        legacy = mock(LegacyClient.class);
        verify = mock(VerificationStore.class);
        backoff = mock(BackoffPolicy.class);
        metrics = mock(MetricsRegistry.class);
        when(throttle.acquireBlocking()).thenReturn(Uni.createFrom().voidItem());
        when(backoff.delayForAttempt(anyInt())).thenReturn(Duration.ofMillis(1));
        orchestrator = new RetryOrchestrator(throttle, legacy, verify, backoff, metrics, 3);
    }

    private MessageEnvelope env() {
        return new MessageEnvelope("M1", "k1", Instant.now(), 0, null,
            new MessageEnvelope.Metadata(null, "users"), Map.of("a", 1));
    }

    private Uni<LegacyResponse> ok() {
        return Uni.createFrom().item(new LegacyResponse(Map.of("ok", true)));
    }

    private Uni<LegacyResponse> timeout() {
        return Uni.createFrom().failure(new TimeoutException("timed out"));
    }

    @Test
    void success_on_first_call_acks_without_checking_table() {
        when(legacy.send(eq("users"), eq("k1"), any())).thenReturn(ok());

        orchestrator.execute(env()).await().indefinitely();

        verify(legacy, times(1)).send(any(), any(), any());
        verifyNoInteractions(verify);
    }

    @Test
    void timeout_then_record_found_acks() {
        when(legacy.send(any(), any(), any())).thenReturn(timeout());
        when(verify.exists("k1")).thenReturn(Uni.createFrom().item(true));

        orchestrator.execute(env()).await().indefinitely();

        verify(legacy, times(1)).send(any(), any(), any());
        verify(verify, times(1)).exists("k1");
        verify(metrics).verifyChecked("found");
    }

    @Test
    void transient_5xx_then_record_found_acks() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new LegacyTransientException(503, "boom")));
        when(verify.exists("k1")).thenReturn(Uni.createFrom().item(true));

        orchestrator.execute(env()).await().indefinitely();

        verify(legacy, times(1)).send(any(), any(), any());
        verify(verify, times(1)).exists("k1");
    }

    @Test
    void timeout_three_times_with_empty_table_exhausts_and_fails() {
        when(legacy.send(any(), any(), any())).thenReturn(timeout());
        when(verify.exists("k1")).thenReturn(Uni.createFrom().item(false));

        assertThatThrownBy(() -> orchestrator.execute(env()).await().indefinitely())
            .isInstanceOf(TimeoutException.class);

        verify(legacy, times(3)).send(any(), any(), any());
        verify(verify, times(3)).exists("k1");
        verify(throttle, times(3)).acquireBlocking();
        verify(metrics, times(2)).apiRetried();
    }

    @Test
    void permanent_4xx_fails_immediately_without_wait_or_check() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new LegacyPermanentException(400, "bad")));

        assertThatThrownBy(() -> orchestrator.execute(env()).await().indefinitely())
            .isInstanceOf(LegacyPermanentException.class);

        verify(legacy, times(1)).send(any(), any(), any());
        verifyNoInteractions(verify);
        verifyNoInteractions(backoff);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -q test -Dtest=RetryOrchestratorTest`
Expected: FAIL — `RetryOrchestrator` does not exist (compilation error).

- [ ] **Step 3: Create `RetryOrchestrator`**

Create `src/main/java/com/throttling/processing/RetryOrchestrator.java`:

```java
package com.throttling.processing;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import com.throttling.common.MessageEnvelope;
import com.throttling.legacy.LegacyClient;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.TokenBucketService;
import com.throttling.verification.VerificationStore;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.rest.client.inject.RestClient;

@ApplicationScoped
public class RetryOrchestrator {

    private final TokenBucketService throttle;
    private final LegacyClient legacy;
    private final VerificationStore verify;
    private final BackoffPolicy backoff;
    private final MetricsRegistry metrics;
    private final int maxAttempts;

    @Inject
    public RetryOrchestrator(TokenBucketService throttle,
                             @RestClient LegacyClient legacy,
                             VerificationStore verify,
                             BackoffPolicy backoff,
                             MetricsRegistry metrics,
                             VerifyConfig config) {
        this(throttle, legacy, verify, backoff, metrics, config.maxAttempts());
    }

    public RetryOrchestrator(TokenBucketService throttle,
                             LegacyClient legacy,
                             VerificationStore verify,
                             BackoffPolicy backoff,
                             MetricsRegistry metrics,
                             int maxAttempts) {
        this.throttle = throttle;
        this.legacy = legacy;
        this.verify = verify;
        this.backoff = backoff;
        this.metrics = metrics;
        this.maxAttempts = maxAttempts;
    }

    public Uni<Void> execute(MessageEnvelope env) {
        return attempt(env, 1);
    }

    private Uni<Void> attempt(MessageEnvelope env, int n) {
        String endpoint = env.metadata() != null && env.metadata().targetEndpoint() != null
            ? env.metadata().targetEndpoint()
            : "default";
        return throttle.acquireBlocking()
            .chain(() -> legacy.send(endpoint, env.idempotencyKey(), env.payload()))
            .replaceWithVoid()
            .onFailure().recoverWithUni(err -> handleFailure(env, n, err));
    }

    private Uni<Void> handleFailure(MessageEnvelope env, int n, Throwable err) {
        if (!isRetriable(err)) {
            return Uni.createFrom().failure(err);
        }
        Duration delay = backoff.delayForAttempt(n);
        return Uni.createFrom().voidItem()
            .onItem().delayIt().by(delay)
            .chain(() -> verify.exists(env.idempotencyKey()))
            .chain(found -> {
                if (Boolean.TRUE.equals(found)) {
                    if (metrics != null) metrics.verifyChecked("found");
                    return Uni.createFrom().voidItem();
                }
                if (metrics != null) metrics.verifyChecked("empty");
                if (n < maxAttempts) {
                    if (metrics != null) metrics.apiRetried();
                    return attempt(env, n + 1);
                }
                return Uni.createFrom().<Void>failure(err);
            });
    }

    private boolean isRetriable(Throwable err) {
        Throwable t = err;
        while (t != null) {
            if (t instanceof LegacyTransientException
                || t instanceof TimeoutException
                || t instanceof org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException) {
                return true;
            }
            if (t == t.getCause()) break;
            t = t.getCause();
        }
        return false;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -q test -Dtest=RetryOrchestratorTest`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/throttling/processing/RetryOrchestrator.java \
        src/test/java/com/throttling/processing/RetryOrchestratorTest.java
git commit -m "feat(processing): add RetryOrchestrator for verify-then-retry on timeout"
```

---

## Task 6: Wire orchestrator into MessageHandler + remove blind @Retry

**Files:**
- Modify: `src/test/java/com/throttling/processing/MessageHandlerTest.java`
- Modify: `src/main/java/com/throttling/processing/MessageHandler.java`
- Modify: `src/main/java/com/throttling/legacy/LegacyClient.java`

- [ ] **Step 1: Rewrite `MessageHandlerTest` to mock the orchestrator (failing)**

Replace the entire contents of `src/test/java/com/throttling/processing/MessageHandlerTest.java` with:

```java
package com.throttling.processing;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import com.throttling.dlq.DlqProducer;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.ThrottleTimeoutException;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MessageHandlerTest {

    RetryOrchestrator orchestrator;
    DlqProducer dlq;
    MetricsRegistry metrics;
    MessageHandler handler;

    @BeforeEach
    void setup() {
        orchestrator = mock(RetryOrchestrator.class);
        dlq = mock(DlqProducer.class);
        metrics = mock(MetricsRegistry.class);
        handler = new MessageHandler(orchestrator, dlq, metrics);
        when(dlq.send(any(), any(), any(), anyInt())).thenReturn(Uni.createFrom().voidItem());
    }

    private MessageEnvelope env() {
        return new MessageEnvelope("M1", "k1", Instant.now(), 0, null,
            new MessageEnvelope.Metadata(null, "users"), Map.of("a", 1));
    }

    @Test
    void acks_and_counts_success_when_orchestrator_completes() {
        when(orchestrator.execute(any())).thenReturn(Uni.createFrom().voidItem());

        handler.handle(env()).await().indefinitely();

        verify(metrics).consumed("success");
        verifyNoInteractions(dlq);
    }

    @Test
    void sends_to_dlq_with_circuit_open_reason() {
        when(orchestrator.execute(any()))
            .thenReturn(Uni.createFrom().failure(new CircuitBreakerOpenException("open")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.CIRCUIT_OPEN);
    }

    @Test
    void sends_to_dlq_with_legacy_5xx_reason() {
        when(orchestrator.execute(any()))
            .thenReturn(Uni.createFrom().failure(new LegacyTransientException(503, "x")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.LEGACY_5XX);
    }

    @Test
    void sends_to_dlq_with_permanent_4xx_reason() {
        when(orchestrator.execute(any()))
            .thenReturn(Uni.createFrom().failure(new LegacyPermanentException(400, "x")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.LEGACY_4XX_PERMANENT);
    }

    @Test
    void sends_to_dlq_with_throttle_timeout_reason() {
        when(orchestrator.execute(any()))
            .thenReturn(Uni.createFrom().failure(new ThrottleTimeoutException()));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.THROTTLE_TIMEOUT);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -q test -Dtest=MessageHandlerTest`
Expected: FAIL — `MessageHandler` constructor still takes the old `(throttle, legacy, dlq, metrics)` signature (compilation error).

- [ ] **Step 3: Rewrite `MessageHandler` to delegate to the orchestrator**

Replace the entire contents of `src/main/java/com/throttling/processing/MessageHandler.java` with:

```java
package com.throttling.processing;

import java.util.concurrent.TimeoutException;

import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import com.throttling.dlq.DlqProducer;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.observability.MetricsRegistry;
import com.throttling.throttling.ThrottleTimeoutException;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MessageHandler {

    private final RetryOrchestrator orchestrator;
    private final DlqProducer dlq;
    private final MetricsRegistry metrics;

    @Inject
    public MessageHandler(RetryOrchestrator orchestrator,
                          DlqProducer dlq,
                          MetricsRegistry metrics) {
        this.orchestrator = orchestrator;
        this.dlq = dlq;
        this.metrics = metrics;
    }

    public Uni<Void> handle(MessageEnvelope env) {
        return orchestrator.execute(env)
            .invoke(() -> { if (metrics != null) metrics.consumed("success"); })
            .onFailure().recoverWithUni(err -> sendToDlq(env, err));
    }

    private Uni<Void> sendToDlq(MessageEnvelope env, Throwable err) {
        FailureReason reason = classify(err);
        if (metrics != null) { metrics.consumed("dlq"); metrics.dlqSent(reason.name()); }
        return dlq.send(env, reason, err.getMessage(), env.attempt());
    }

    private FailureReason classify(Throwable err) {
        Throwable t = unwrap(err);
        if (t instanceof CircuitBreakerOpenException) return FailureReason.CIRCUIT_OPEN;
        if (t instanceof ThrottleTimeoutException) return FailureReason.THROTTLE_TIMEOUT;
        if (t instanceof LegacyPermanentException) return FailureReason.LEGACY_4XX_PERMANENT;
        if (t instanceof LegacyTransientException) return FailureReason.LEGACY_5XX;
        if (t instanceof TimeoutException
            || t instanceof org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException) {
            return FailureReason.LEGACY_TIMEOUT;
        }
        return FailureReason.LEGACY_5XX;
    }

    private Throwable unwrap(Throwable t) {
        while (t.getCause() != null && t != t.getCause()) {
            if (t instanceof CircuitBreakerOpenException || t instanceof ThrottleTimeoutException
                || t instanceof LegacyTransientException || t instanceof LegacyPermanentException) {
                return t;
            }
            t = t.getCause();
        }
        return t;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -q test -Dtest=MessageHandlerTest`
Expected: PASS (5 tests).

- [ ] **Step 5: Remove the blind `@Retry` from `LegacyClient`**

In `src/main/java/com/throttling/legacy/LegacyClient.java`, delete the `@Retry(...)` annotation block on the `send` method (lines 28-31):

```java
    @Retry(maxRetries = 3,
           delay = 200, delayUnit = ChronoUnit.MILLIS,
           jitter = 100, jitterDelayUnit = ChronoUnit.MILLIS,
           retryOn = { LegacyTransientException.class, TimeoutException.class })
```

Then delete the now-unused import on line 11:

```java
import org.eclipse.microprofile.faulttolerance.Retry;
```

Keep all other annotations (`@CircuitBreaker`, `@CircuitBreakerName`, `@Timeout`, `@Bulkhead`) and the `ChronoUnit`, `TimeoutException`, `LegacyTransientException` imports — they are still referenced by `@CircuitBreaker`.

- [ ] **Step 6: Run the full test suite**

Run: `./mvnw -q test`
Expected: PASS — all unit tests green (Docker must be available for `PgVerificationStoreTest`).

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/throttling/processing/MessageHandler.java \
        src/test/java/com/throttling/processing/MessageHandlerTest.java \
        src/main/java/com/throttling/legacy/LegacyClient.java
git commit -m "refactor(processing): route consumer through RetryOrchestrator; drop blind client retry"
```

---

## Task 7: Mock legacy writes the record + simulates timeout (dev/console)

**Files:**
- Modify: `src/main/java/com/throttling/legacy/MockLegacyResource.java`

This task wires the dev mock so the throttle test console can exercise the
timeout→found→ack path. No automated test — `MockLegacyResource` is `@IfBuildProfile("dev")`;
verification is manual via the running app.

- [ ] **Step 1: Rewrite `MockLegacyResource`**

Replace the entire contents of `src/main/java/com/throttling/legacy/MockLegacyResource.java` with:

```java
package com.throttling.legacy;

import com.throttling.verification.ProcessingRecord;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

@Path("/dev-legacy")
@ApplicationScoped
@IfBuildProfile("dev")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MockLegacyResource {

    private static final Logger LOG = Logger.getLogger(MockLegacyResource.class);

    @POST
    @Path("{endpoint}")
    public Uni<Response> handle(
            @PathParam("endpoint") String endpoint,
            @HeaderParam("Idempotency-Key") String idempKey,
            Map<String, Object> payload) {
        boolean slow = payload != null && Boolean.TRUE.equals(payload.get("simulateTimeout"));
        LOG.debugf("mock-legacy: endpoint=%s idempKey=%s simulateTimeout=%s", endpoint, idempKey, slow);

        Response ok = Response.ok(Map.of("status", "ok", "endpoint", endpoint)).build();
        return writeRecord(idempKey)
            .chain(() -> slow
                ? Uni.createFrom().item(ok).onItem().delayIt().by(Duration.ofSeconds(7))
                : Uni.createFrom().item(ok));
    }

    /** Simulates the legado persisting its processing record, keyed by the idempotency key. */
    private Uni<Void> writeRecord(String idempKey) {
        if (idempKey == null) return Uni.createFrom().voidItem();
        return Panache.withTransaction(() ->
            ProcessingRecord.<ProcessingRecord>findById(idempKey)
                .chain(existing -> {
                    if (existing != null) return Uni.createFrom().voidItem();
                    ProcessingRecord rec = new ProcessingRecord();
                    rec.idempotencyKey = idempKey;
                    rec.processedAt = Instant.now();
                    return rec.persist().replaceWithVoid();
                }));
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `./mvnw -q compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Manual end-to-end verification (dev profile)**

Start infra and the app:

```bash
docker compose up -d postgres redis kafka
./mvnw quarkus:dev
```

Then, via the throttle test console (or `curl` to the ingress endpoint), send a message
whose payload contains `"simulateTimeout": true`. Expected behavior:
- The mock writes a `processing_record` row, then stalls 7s.
- The REST client hits its 5s `@Timeout` → `RetryOrchestrator` waits the backoff, checks
  the table, finds the row, and the message is **acked** (no DLQ).
- Confirm via metrics: `legacy_verify_checked_total{result="found"}` increments and
  `dlq_messages_sent_total` does not.

For the DLQ path, point `quarkus.rest-client.legacy-api.url` at a stub that times out
without writing the row (or stop Postgres writes), send a normal message, and confirm the
message lands on `messages.dlq` after 3 attempts.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/throttling/legacy/MockLegacyResource.java
git commit -m "feat(dev): mock legacy writes processing_record and can simulate timeout"
```

---

## Final verification

- [ ] **Run the full build**

Run: `./mvnw -q verify`
Expected: BUILD SUCCESS — all unit tests pass (Docker required for the Postgres-backed test). Integration tests (`*IT`) run under failsafe.

---

## Notes for the implementer

- **Reactive sessions:** Hibernate Reactive operations must run on a Vert.x context. The
  Kafka consumer pipeline already runs reactively, so `Panache.withSession(...)` inside
  `PgVerificationStore` is fine in production. In tests, use `@RunOnVertxContext` +
  `UniAsserter` (Task 3) — do **not** call `.await()` on Panache operations off a Vert.x
  context.
- **Why the test-only constructor on `RetryOrchestrator`:** mirrors `TokenBucketService`,
  letting unit tests inject a fixed `maxAttempts` and mocked collaborators without CDI.
- **Throttle is now per-attempt:** each of the up-to-3 API calls re-acquires a token, so a
  retried message respects the rate limit on every external call.
- **`delayIt().by(Duration)`** requires a positive duration; tests stub the backoff to
  `Duration.ofMillis(1)` to stay fast.
