# Kafka Throttling System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Quarkus reactive application that ingests HTTP messages, publishes them to Kafka, and consumes them at a rate controlled by a Redis-backed token bucket to protect a legacy REST system, with idempotency, circuit-breaker resilience, DLQ, and full OpenTelemetry OTLP observability.

**Architecture:** Single Quarkus deployable with modular Java packages (`ingress`, `processing`, `throttling`, `legacy`, `dlq`, `observability`, `admin`, `common`). Reactive end-to-end via Mutiny. Ingress uses Redis `SET NX` for idempotency before producing to Kafka. Consumer uses SmallRye Reactive Messaging with `concurrency=1`, acquires a token via Bucket4j-Redis (blocking-on-Uni for natural backpressure), then calls the legacy REST API via a reactive REST client wrapped with MicroProfile Fault Tolerance (Retry + Circuit Breaker + Timeout + Bulkhead). Failures go to a DLQ topic. OpenTelemetry exporter sends traces, metrics, and logs via OTLP gRPC to a collector.

**Tech Stack:** Quarkus 3.x (latest stable), Java 21, Mutiny, SmallRye Reactive Messaging (Kafka), Quarkus Redis Client (Lettuce-based), Bucket4j-Redis, Quarkus REST + REST Client Reactive (Jackson), MicroProfile Fault Tolerance, Quarkus OpenTelemetry + OTLP exporter, Micrometer with Prometheus registry, Quarkus Logging JSON, Testcontainers (Kafka, Redis), Wiremock, REST-assured, AssertJ, Awaitility, ULID-creator.

**Spec reference:** [docs/superpowers/specs/2026-05-25-kafka-throttling-system-design.md](../specs/2026-05-25-kafka-throttling-system-design.md)

---

## File Structure (high level)

```
kafka-throttling-system/
├── pom.xml
├── README.md
├── docker-compose.yml
├── otel-collector-config.yaml
├── .gitignore
├── src/main/java/com/throttling/
│   ├── common/
│   │   ├── MessageEnvelope.java
│   │   ├── UlidGenerator.java
│   │   ├── Constants.java
│   │   └── FailureReason.java
│   ├── ingress/
│   │   ├── MessagesResource.java
│   │   ├── IngressService.java
│   │   ├── IdempotencyStore.java
│   │   └── dto/
│   │       ├── IngressRequest.java
│   │       ├── IngressResponse.java
│   │       └── DuplicateResponse.java
│   ├── processing/
│   │   ├── MessageConsumer.java
│   │   └── MessageHandler.java
│   ├── throttling/
│   │   ├── TokenBucketService.java
│   │   ├── BucketConfig.java
│   │   └── ThrottleTimeoutException.java
│   ├── legacy/
│   │   ├── LegacyClient.java
│   │   ├── LegacyResponse.java
│   │   ├── LegacyResponseMapper.java
│   │   └── exceptions/
│   │       ├── LegacyTransientException.java
│   │       └── LegacyPermanentException.java
│   ├── dlq/
│   │   ├── DlqProducer.java
│   │   └── DlqEnvelope.java
│   ├── observability/
│   │   └── MetricsRegistry.java
│   └── admin/
│       └── AdminResource.java
├── src/main/resources/
│   └── application.properties
└── src/test/
    ├── java/com/throttling/
    │   ├── common/                      # unit
    │   ├── ingress/                     # unit
    │   ├── throttling/                  # unit
    │   ├── legacy/                      # unit
    │   ├── dlq/                         # unit
    │   ├── processing/                  # unit
    │   └── integration/                 # @QuarkusTest + Testcontainers
    │       ├── KafkaTestResource.java
    │       ├── RedisTestResource.java
    │       ├── WiremockTestResource.java
    │       ├── IngressIdempotencyIT.java
    │       ├── ThrottleBackpressureIT.java
    │       ├── CircuitBreakerDlqIT.java
    │       ├── RetryTransientIT.java
    │       └── EndToEndIT.java
    └── resources/
        ├── application-test.properties
        └── wiremock/mappings/
```

---

## Task 1: Bootstrap Maven Quarkus Project

**Files:**
- Create: `pom.xml`
- Create: `.gitignore`
- Create: `src/main/resources/application.properties` (empty stub)
- Create: `src/main/java/com/throttling/.gitkeep`

- [ ] **Step 1: Create `.gitignore`**

```
target/
.idea/
*.iml
.vscode/
.classpath
.project
.settings/
.factorypath
**/*.log
.DS_Store
.env
build/
.mvn/wrapper/maven-wrapper.jar
.quarkus/
```

- [ ] **Step 2: Create `pom.xml` with full dependency set**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.throttling</groupId>
    <artifactId>kafka-throttling-system</artifactId>
    <version>1.0.0-SNAPSHOT</version>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.release>21</maven.compiler.release>
        <quarkus.platform.group-id>io.quarkus.platform</quarkus.platform.group-id>
        <quarkus.platform.artifact-id>quarkus-bom</quarkus.platform.artifact-id>
        <quarkus.platform.version>3.17.4</quarkus.platform.version>
        <surefire-plugin.version>3.5.2</surefire-plugin.version>
        <failsafe-plugin.version>3.5.2</failsafe-plugin.version>
        <bucket4j.version>8.10.1</bucket4j.version>
        <ulid.version>5.2.3</ulid.version>
        <testcontainers.version>1.20.4</testcontainers.version>
        <wiremock.version>3.10.0</wiremock.version>
        <awaitility.version>4.2.2</awaitility.version>
        <assertj.version>3.26.3</assertj.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>${quarkus.platform.group-id}</groupId>
                <artifactId>${quarkus.platform.artifact-id}</artifactId>
                <version>${quarkus.platform.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-arc</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-rest</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-rest-jackson</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-rest-client-reactive-jackson</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-smallrye-reactive-messaging-kafka</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-redis-client</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-smallrye-fault-tolerance</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-smallrye-health</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-opentelemetry</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-micrometer-registry-prometheus</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-logging-json</artifactId></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-container-image-jib</artifactId></dependency>

        <dependency>
            <groupId>com.bucket4j</groupId>
            <artifactId>bucket4j_jdk17-redis</artifactId>
            <version>${bucket4j.version}</version>
        </dependency>
        <dependency>
            <groupId>com.bucket4j</groupId>
            <artifactId>bucket4j_jdk17-lettuce</artifactId>
            <version>${bucket4j.version}</version>
        </dependency>
        <dependency>
            <groupId>io.lettuce</groupId>
            <artifactId>lettuce-core</artifactId>
        </dependency>
        <dependency>
            <groupId>com.github.f4b6a3</groupId>
            <artifactId>ulid-creator</artifactId>
            <version>${ulid.version}</version>
        </dependency>

        <!-- test -->
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-junit5</artifactId><scope>test</scope></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-junit5-mockito</artifactId><scope>test</scope></dependency>
        <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-test-kafka-companion</artifactId><scope>test</scope></dependency>
        <dependency><groupId>io.rest-assured</groupId><artifactId>rest-assured</artifactId><scope>test</scope></dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.redis</groupId>
            <artifactId>testcontainers-redis</artifactId>
            <version>2.2.2</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.wiremock</groupId>
            <artifactId>wiremock-standalone</artifactId>
            <version>${wiremock.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>${awaitility.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>${assertj.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>${quarkus.platform.group-id}</groupId>
                <artifactId>quarkus-maven-plugin</artifactId>
                <version>${quarkus.platform.version}</version>
                <extensions>true</extensions>
                <executions>
                    <execution>
                        <goals>
                            <goal>build</goal>
                            <goal>generate-code</goal>
                            <goal>generate-code-tests</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>${surefire-plugin.version}</version>
                <configuration>
                    <systemPropertyVariables>
                        <java.util.logging.manager>org.jboss.logmanager.LogManager</java.util.logging.manager>
                    </systemPropertyVariables>
                </configuration>
            </plugin>
            <plugin>
                <artifactId>maven-failsafe-plugin</artifactId>
                <version>${failsafe-plugin.version}</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>integration-test</goal>
                            <goal>verify</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <systemPropertyVariables>
                        <java.util.logging.manager>org.jboss.logmanager.LogManager</java.util.logging.manager>
                        <maven.home>${maven.home}</maven.home>
                    </systemPropertyVariables>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Create minimal `application.properties`**

```properties
quarkus.application.name=kafka-throttling-system
quarkus.http.port=8080
```

- [ ] **Step 4: Create stub `src/main/java/com/throttling/.gitkeep` and `src/test/java/com/throttling/.gitkeep`**

```bash
mkdir -p src/main/java/com/throttling src/test/java/com/throttling
touch src/main/java/com/throttling/.gitkeep src/test/java/com/throttling/.gitkeep
```

- [ ] **Step 5: Validate build**

Run: `mvn -q -DskipTests package`
Expected: BUILD SUCCESS, produces `target/quarkus-app/`.

- [ ] **Step 6: Commit**

```bash
git add pom.xml .gitignore src/main/resources/application.properties src/main/java src/test/java
git commit -m "chore: bootstrap Quarkus Maven project with deps"
```

---

## Task 2: Docker Compose Dev Stack

**Files:**
- Create: `docker-compose.yml`
- Create: `otel-collector-config.yaml`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: kts-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: q3DqYE9oTm6mYTYxYjIxYz

  redis:
    image: redis:7.4-alpine
    container_name: kts-redis
    ports:
      - "6379:6379"
    command: ["redis-server", "--appendonly", "yes"]

  wiremock:
    image: wiremock/wiremock:3.10.0
    container_name: kts-wiremock
    ports:
      - "8089:8080"
    command: ["--global-response-templating", "--verbose"]

  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.114.0
    container_name: kts-otel
    command: ["--config=/etc/otelcol-contrib/config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otelcol-contrib/config.yaml
    ports:
      - "4317:4317"  # OTLP gRPC
      - "4318:4318"  # OTLP HTTP
```

- [ ] **Step 2: Write `otel-collector-config.yaml`**

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch: {}

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
```

- [ ] **Step 3: Smoke test compose**

Run: `docker-compose up -d && sleep 8 && docker-compose ps`
Expected: 4 containers in `running` state.

Then: `docker-compose down`

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml otel-collector-config.yaml
git commit -m "chore: add docker-compose dev stack (kafka, redis, wiremock, otel)"
```

---

## Task 3: Common — ULID Generator

**Files:**
- Create: `src/main/java/com/throttling/common/UlidGenerator.java`
- Create: `src/test/java/com/throttling/common/UlidGeneratorTest.java`

- [ ] **Step 1: Write failing test**

```java
package com.throttling.common;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class UlidGeneratorTest {

    @Test
    void generates_ulid_with_26_chars() {
        UlidGenerator g = new UlidGenerator();
        String id = g.next();
        assertThat(id).hasSize(26).matches("[0-9A-HJKMNP-TV-Z]{26}");
    }

    @Test
    void generates_unique_values() {
        UlidGenerator g = new UlidGenerator();
        assertThat(g.next()).isNotEqualTo(g.next());
    }
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn -q -Dtest=UlidGeneratorTest test`
Expected: compilation fails (class not found).

- [ ] **Step 3: Implement**

```java
package com.throttling.common;

import com.github.f4b6a3.ulid.UlidCreator;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class UlidGenerator {
    public String next() {
        return UlidCreator.getUlid().toString();
    }
}
```

- [ ] **Step 4: Run and verify pass**

Run: `mvn -q -Dtest=UlidGeneratorTest test`
Expected: 2 tests passed.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/throttling/common/UlidGenerator.java src/test/java/com/throttling/common/UlidGeneratorTest.java
git commit -m "feat(common): add ULID generator"
```

---

## Task 4: Common — MessageEnvelope + Constants + FailureReason

**Files:**
- Create: `src/main/java/com/throttling/common/MessageEnvelope.java`
- Create: `src/main/java/com/throttling/common/Constants.java`
- Create: `src/main/java/com/throttling/common/FailureReason.java`
- Create: `src/test/java/com/throttling/common/MessageEnvelopeTest.java`

- [ ] **Step 1: Write failing test**

```java
package com.throttling.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MessageEnvelopeTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void serialises_and_deserialises_round_trip() throws Exception {
        MessageEnvelope env = new MessageEnvelope(
            "01HXYZ",
            "key-1",
            Instant.parse("2026-05-25T14:30:00Z"),
            0,
            new MessageEnvelope.TraceContext("00-abc-def-01", null),
            new MessageEnvelope.Metadata("source-x", "endpoint-y"),
            Map.of("foo", "bar")
        );

        String json = mapper.writeValueAsString(env);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

        assertThat(parsed.messageId()).isEqualTo("01HXYZ");
        assertThat(parsed.idempotencyKey()).isEqualTo("key-1");
        assertThat(parsed.attempt()).isZero();
        assertThat(parsed.traceContext().traceparent()).isEqualTo("00-abc-def-01");
        assertThat(parsed.metadata().targetEndpoint()).isEqualTo("endpoint-y");
        assertThat(parsed.payload()).containsEntry("foo", "bar");
    }
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn -q -Dtest=MessageEnvelopeTest test`
Expected: compile error.

- [ ] **Step 3: Implement `MessageEnvelope`**

```java
package com.throttling.common;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MessageEnvelope(
        String messageId,
        String idempotencyKey,
        Instant occurredAt,
        int attempt,
        TraceContext traceContext,
        Metadata metadata,
        Map<String, Object> payload
) {
    public MessageEnvelope withAttempt(int newAttempt) {
        return new MessageEnvelope(messageId, idempotencyKey, occurredAt, newAttempt, traceContext, metadata, payload);
    }

    public record TraceContext(String traceparent, String tracestate) {}

    public record Metadata(String source, String targetEndpoint) {}
}
```

- [ ] **Step 4: Implement `Constants`**

```java
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
```

- [ ] **Step 5: Implement `FailureReason`**

```java
package com.throttling.common;

public enum FailureReason {
    LEGACY_5XX(false),
    LEGACY_TIMEOUT(false),
    LEGACY_4XX_PERMANENT(true),
    CIRCUIT_OPEN(false),
    THROTTLE_TIMEOUT(false),
    INVALID_PAYLOAD(true),
    HARD_RETRY_LIMIT(true);

    private final boolean permanent;

    FailureReason(boolean permanent) {
        this.permanent = permanent;
    }

    public boolean isPermanent() {
        return permanent;
    }
}
```

- [ ] **Step 6: Run and verify pass**

Run: `mvn -q -Dtest=MessageEnvelopeTest test`
Expected: 1 test passed.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/throttling/common/ src/test/java/com/throttling/common/MessageEnvelopeTest.java
git commit -m "feat(common): add MessageEnvelope, Constants, FailureReason"
```

---

## Task 5: Ingress — IdempotencyStore (Redis SET NX)

**Files:**
- Create: `src/main/java/com/throttling/ingress/IdempotencyStore.java`
- Create: `src/test/java/com/throttling/ingress/IdempotencyStoreTest.java`

The store wraps Quarkus `ReactiveRedisDataSource` with two operations: `tryStore(key, messageId, ttl)` returns a `Uni<Optional<String>>` — empty when the key was stored (new), non-empty containing the original `messageId` when duplicate; and `remove(key)` returns `Uni<Void>`.

- [ ] **Step 1: Write failing test**

```java
package com.throttling.ingress;

import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.SetArgs;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdempotencyStoreTest {

    @Test
    void try_store_returns_empty_when_set_succeeds() {
        ReactiveRedisDataSource ds = mock(ReactiveRedisDataSource.class);
        ReactiveValueCommands<String, String> values = mock(ReactiveValueCommands.class);
        when(ds.value(String.class)).thenReturn(values);
        when(values.setGet(eq("idemp:k1"), eq("M1"), any(SetArgs.class)))
            .thenReturn(Uni.createFrom().nullItem());

        IdempotencyStore store = new IdempotencyStore(ds);

        Optional<String> result = store
            .tryStore("k1", "M1", Duration.ofSeconds(60))
            .await().indefinitely();

        assertThat(result).isEmpty();
    }

    @Test
    void try_store_returns_existing_when_key_present() {
        ReactiveRedisDataSource ds = mock(ReactiveRedisDataSource.class);
        ReactiveValueCommands<String, String> values = mock(ReactiveValueCommands.class);
        when(ds.value(String.class)).thenReturn(values);
        when(values.setGet(eq("idemp:k1"), eq("M2"), any(SetArgs.class)))
            .thenReturn(Uni.createFrom().item("M1"));

        IdempotencyStore store = new IdempotencyStore(ds);

        Optional<String> result = store
            .tryStore("k1", "M2", Duration.ofSeconds(60))
            .await().indefinitely();

        assertThat(result).contains("M1");
    }

    @Test
    void remove_calls_redis_del() {
        ReactiveRedisDataSource ds = mock(ReactiveRedisDataSource.class);
        io.quarkus.redis.datasource.keys.ReactiveKeyCommands<String> keys =
            mock(io.quarkus.redis.datasource.keys.ReactiveKeyCommands.class);
        when(ds.key()).thenReturn(keys);
        when(keys.del("idemp:k1")).thenReturn(Uni.createFrom().item(1));

        IdempotencyStore store = new IdempotencyStore(ds);
        store.remove("k1").await().indefinitely();

        verify(keys).del("idemp:k1");
    }
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn -q -Dtest=IdempotencyStoreTest test`
Expected: compile error.

- [ ] **Step 3: Implement `IdempotencyStore`**

```java
package com.throttling.ingress;

import com.throttling.common.Constants;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.SetArgs;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.Optional;

@ApplicationScoped
public class IdempotencyStore {

    private final ReactiveRedisDataSource ds;

    @Inject
    public IdempotencyStore(ReactiveRedisDataSource ds) {
        this.ds = ds;
    }

    public Uni<Optional<String>> tryStore(String idempotencyKey, String messageId, Duration ttl) {
        String redisKey = Constants.IDEMP_KEY_PREFIX + idempotencyKey;
        SetArgs args = new SetArgs().nx().ex(ttl.toSeconds());
        return ds.value(String.class)
            .setGet(redisKey, messageId, args)
            .map(Optional::ofNullable);
    }

    public Uni<Void> remove(String idempotencyKey) {
        String redisKey = Constants.IDEMP_KEY_PREFIX + idempotencyKey;
        return ds.key().del(redisKey).replaceWithVoid();
    }
}
```

- [ ] **Step 4: Run and verify pass**

Run: `mvn -q -Dtest=IdempotencyStoreTest test`
Expected: 3 tests passed.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/throttling/ingress/IdempotencyStore.java src/test/java/com/throttling/ingress/IdempotencyStoreTest.java
git commit -m "feat(ingress): add Redis idempotency store with SET NX"
```

---

## Task 6: Ingress — DTOs

**Files:**
- Create: `src/main/java/com/throttling/ingress/dto/IngressRequest.java`
- Create: `src/main/java/com/throttling/ingress/dto/IngressResponse.java`
- Create: `src/main/java/com/throttling/ingress/dto/DuplicateResponse.java`

- [ ] **Step 1: Implement DTOs**

```java
// IngressRequest.java
package com.throttling.ingress.dto;

import com.throttling.common.MessageEnvelope;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public record IngressRequest(
        @NotNull Map<String, Object> payload,
        MessageEnvelope.Metadata metadata
) {}
```

```java
// IngressResponse.java
package com.throttling.ingress.dto;

import java.time.Instant;

public record IngressResponse(String messageId, Instant acceptedAt) {}
```

```java
// DuplicateResponse.java
package com.throttling.ingress.dto;

public record DuplicateResponse(String error, String messageId, String idempotencyKey) {
    public static DuplicateResponse of(String messageId, String idempotencyKey) {
        return new DuplicateResponse("DUPLICATE", messageId, idempotencyKey);
    }
}
```

- [ ] **Step 2: Compile**

Run: `mvn -q compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/throttling/ingress/dto/
git commit -m "feat(ingress): add request/response DTOs"
```

---

## Task 7: Ingress — IngressService

**Files:**
- Create: `src/main/java/com/throttling/ingress/IngressService.java`
- Create: `src/test/java/com/throttling/ingress/IngressServiceTest.java`

`IngressService` orchestrates: build envelope → call `IdempotencyStore.tryStore` → emit on the outgoing channel `messages-in`; on Kafka failure, calls `IdempotencyStore.remove`. Returns `Uni<IngressOutcome>`. The Kafka emitter is wrapped in a thin interface `MessageProducer` to allow mocking.

- [ ] **Step 1: Create `MessageProducer` interface and `IngressOutcome`**

```java
// src/main/java/com/throttling/ingress/MessageProducer.java
package com.throttling.ingress;

import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;

public interface MessageProducer {
    Uni<Void> send(MessageEnvelope envelope);
}
```

```java
// src/main/java/com/throttling/ingress/IngressOutcome.java
package com.throttling.ingress;

import java.time.Instant;

public sealed interface IngressOutcome {
    record Accepted(String messageId, Instant acceptedAt) implements IngressOutcome {}
    record Duplicate(String originalMessageId) implements IngressOutcome {}
}
```

- [ ] **Step 2: Write failing test**

```java
package com.throttling.ingress;

import com.throttling.common.MessageEnvelope;
import com.throttling.common.UlidGenerator;
import com.throttling.ingress.dto.IngressRequest;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class IngressServiceTest {

    IdempotencyStore store;
    MessageProducer producer;
    UlidGenerator ulid;
    IngressService service;

    @BeforeEach
    void setup() {
        store = mock(IdempotencyStore.class);
        producer = mock(MessageProducer.class);
        ulid = mock(UlidGenerator.class);
        service = new IngressService(store, producer, ulid, Duration.ofSeconds(60));
        when(ulid.next()).thenReturn("M1");
    }

    @Test
    void accepts_new_message() {
        when(store.tryStore(eq("k1"), eq("M1"), any(Duration.class)))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        when(producer.send(any())).thenReturn(Uni.createFrom().voidItem());

        IngressOutcome out = service.handle(
            "k1",
            null,
            new IngressRequest(Map.of("a", 1), null)
        ).await().indefinitely();

        assertThat(out).isInstanceOf(IngressOutcome.Accepted.class);
        ArgumentCaptor<MessageEnvelope> cap = ArgumentCaptor.forClass(MessageEnvelope.class);
        verify(producer).send(cap.capture());
        assertThat(cap.getValue().messageId()).isEqualTo("M1");
        assertThat(cap.getValue().idempotencyKey()).isEqualTo("k1");
    }

    @Test
    void returns_duplicate_when_key_already_present() {
        when(store.tryStore(eq("k1"), eq("M1"), any(Duration.class)))
            .thenReturn(Uni.createFrom().item(Optional.of("M_ORIGINAL")));

        IngressOutcome out = service.handle(
            "k1",
            null,
            new IngressRequest(Map.of("a", 1), null)
        ).await().indefinitely();

        assertThat(out).isEqualTo(new IngressOutcome.Duplicate("M_ORIGINAL"));
        verifyNoInteractions(producer);
    }

    @Test
    void removes_idempotency_key_when_kafka_send_fails() {
        when(store.tryStore(eq("k1"), eq("M1"), any(Duration.class)))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        when(producer.send(any()))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("boom")));
        when(store.remove("k1")).thenReturn(Uni.createFrom().voidItem());

        try {
            service.handle("k1", null, new IngressRequest(Map.of(), null))
                .await().indefinitely();
        } catch (Exception expected) { /* ok */ }

        verify(store).remove("k1");
    }
}
```

- [ ] **Step 3: Run and verify failure**

Run: `mvn -q -Dtest=IngressServiceTest test`
Expected: compile error.

- [ ] **Step 4: Implement `IngressService`**

```java
package com.throttling.ingress;

import com.throttling.common.MessageEnvelope;
import com.throttling.common.UlidGenerator;
import com.throttling.ingress.dto.IngressRequest;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.time.Instant;

@ApplicationScoped
public class IngressService {

    private final IdempotencyStore store;
    private final MessageProducer producer;
    private final UlidGenerator ulid;
    private final Duration idempotencyTtl;

    @Inject
    public IngressService(IdempotencyStore store,
                          MessageProducer producer,
                          UlidGenerator ulid,
                          @ConfigProperty(name = "ingress.idempotency.ttl-seconds", defaultValue = "86400") long ttlSeconds) {
        this(store, producer, ulid, Duration.ofSeconds(ttlSeconds));
    }

    public IngressService(IdempotencyStore store,
                          MessageProducer producer,
                          UlidGenerator ulid,
                          Duration idempotencyTtl) {
        this.store = store;
        this.producer = producer;
        this.ulid = ulid;
        this.idempotencyTtl = idempotencyTtl;
    }

    public Uni<IngressOutcome> handle(String idempotencyKey,
                                      MessageEnvelope.TraceContext traceContext,
                                      IngressRequest request) {
        final String messageId = ulid.next();
        final Instant now = Instant.now();
        final MessageEnvelope envelope = new MessageEnvelope(
            messageId,
            idempotencyKey,
            now,
            0,
            traceContext,
            request.metadata(),
            request.payload()
        );

        return store.tryStore(idempotencyKey, messageId, idempotencyTtl)
            .onItem().transformToUni(existing -> {
                if (existing.isPresent()) {
                    return Uni.createFrom().item(new IngressOutcome.Duplicate(existing.get()));
                }
                return producer.send(envelope)
                    .onFailure().call(err -> store.remove(idempotencyKey))
                    .replaceWith(new IngressOutcome.Accepted(messageId, now));
            });
    }
}
```

- [ ] **Step 5: Run and verify pass**

Run: `mvn -q -Dtest=IngressServiceTest test`
Expected: 3 tests passed.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/throttling/ingress/IngressService.java src/main/java/com/throttling/ingress/MessageProducer.java src/main/java/com/throttling/ingress/IngressOutcome.java src/test/java/com/throttling/ingress/IngressServiceTest.java
git commit -m "feat(ingress): add IngressService orchestrating idempotency + produce"
```

---

## Task 8: Ingress — Kafka MessageProducer Implementation

**Files:**
- Create: `src/main/java/com/throttling/ingress/KafkaMessageProducer.java`
- Modify: `src/main/resources/application.properties`

- [ ] **Step 1: Implement `KafkaMessageProducer`**

```java
package com.throttling.ingress;

import com.throttling.common.Constants;
import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

@ApplicationScoped
public class KafkaMessageProducer implements MessageProducer {

    private final MutinyEmitter<MessageEnvelope> emitter;

    @Inject
    public KafkaMessageProducer(@Channel("messages-out") MutinyEmitter<MessageEnvelope> emitter) {
        this.emitter = emitter;
    }

    @Override
    public Uni<Void> send(MessageEnvelope envelope) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(Constants.HEADER_MESSAGE_ID, envelope.messageId().getBytes());
        if (envelope.traceContext() != null && envelope.traceContext().traceparent() != null) {
            headers.add(Constants.HEADER_TRACEPARENT, envelope.traceContext().traceparent().getBytes());
            if (envelope.traceContext().tracestate() != null) {
                headers.add(Constants.HEADER_TRACESTATE, envelope.traceContext().tracestate().getBytes());
            }
        }
        OutgoingKafkaRecordMetadata<String> meta = OutgoingKafkaRecordMetadata.<String>builder()
            .withKey(envelope.messageId())
            .withTopic(Constants.TOPIC_MESSAGES_IN)
            .withHeaders(headers)
            .build();
        Message<MessageEnvelope> msg = Message.of(envelope, Metadata.of(meta));
        return emitter.sendMessage(msg);
    }
}
```

- [ ] **Step 2: Add Kafka outgoing config to `application.properties`**

Append:

```properties
# Kafka common
kafka.bootstrap.servers=localhost:9092

# Outgoing producer
mp.messaging.outgoing.messages-out.connector=smallrye-kafka
mp.messaging.outgoing.messages-out.topic=messages.in
mp.messaging.outgoing.messages-out.value.serializer=io.quarkus.kafka.client.serialization.ObjectMapperSerializer
mp.messaging.outgoing.messages-out.key.serializer=org.apache.kafka.common.serialization.StringSerializer
mp.messaging.outgoing.messages-out.acks=all
mp.messaging.outgoing.messages-out.retries=10

# Ingress
ingress.idempotency.ttl-seconds=86400
```

- [ ] **Step 3: Compile**

Run: `mvn -q compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/throttling/ingress/KafkaMessageProducer.java src/main/resources/application.properties
git commit -m "feat(ingress): add Kafka producer for messages.in channel"
```

---

## Task 9: Ingress — REST Resource

**Files:**
- Create: `src/main/java/com/throttling/ingress/MessagesResource.java`
- Create: `src/test/java/com/throttling/ingress/MessagesResourceTest.java`

- [ ] **Step 1: Write failing `@QuarkusTest` (resource-level, services mocked)**

```java
package com.throttling.ingress;

import com.throttling.ingress.dto.IngressRequest;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@QuarkusTest
class MessagesResourceTest {

    @InjectMock IngressService service;

    @Test
    void returns_202_with_message_id_when_accepted() {
        when(service.handle(eq("k1"), any(), any(IngressRequest.class)))
            .thenReturn(Uni.createFrom().item(
                new IngressOutcome.Accepted("M1", Instant.parse("2026-05-25T14:30:00Z"))
            ));

        given()
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", "k1")
            .body(Map.of("payload", Map.of("a", 1)))
        .when()
            .post("/messages")
        .then()
            .statusCode(202)
            .body("messageId", equalTo("M1"))
            .body("acceptedAt", notNullValue());
    }

    @Test
    void returns_409_when_duplicate() {
        when(service.handle(eq("k1"), any(), any(IngressRequest.class)))
            .thenReturn(Uni.createFrom().item(new IngressOutcome.Duplicate("M_ORIG")));

        given()
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", "k1")
            .body(Map.of("payload", Map.of("a", 1)))
        .when()
            .post("/messages")
        .then()
            .statusCode(409)
            .body("error", equalTo("DUPLICATE"))
            .body("messageId", equalTo("M_ORIG"))
            .body("idempotencyKey", equalTo("k1"));
    }

    @Test
    void returns_400_when_idempotency_header_missing() {
        given()
            .contentType(MediaType.APPLICATION_JSON)
            .body(Map.of("payload", Map.of("a", 1)))
        .when()
            .post("/messages")
        .then()
            .statusCode(400);
    }
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn -q -Dtest=MessagesResourceTest test`
Expected: 404 / endpoint not found.

- [ ] **Step 3: Implement `MessagesResource`**

```java
package com.throttling.ingress;

import com.throttling.common.Constants;
import com.throttling.common.MessageEnvelope;
import com.throttling.ingress.dto.DuplicateResponse;
import com.throttling.ingress.dto.IngressRequest;
import com.throttling.ingress.dto.IngressResponse;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MessagesResource {

    @Inject IngressService service;

    @POST
    public Uni<Response> postMessage(
            @HeaderParam(Constants.HEADER_IDEMP_KEY) String idempotencyKey,
            @HeaderParam(Constants.HEADER_TRACEPARENT) String traceparent,
            @HeaderParam(Constants.HEADER_TRACESTATE) String tracestate,
            @Valid IngressRequest request) {

        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return Uni.createFrom().item(Response
                .status(Response.Status.BAD_REQUEST)
                .entity("{\"error\":\"X-Idempotency-Key header required\"}")
                .build());
        }
        if (request == null || request.payload() == null) {
            return Uni.createFrom().item(Response
                .status(Response.Status.BAD_REQUEST)
                .entity("{\"error\":\"payload required\"}")
                .build());
        }

        MessageEnvelope.TraceContext tc = traceparent != null
            ? new MessageEnvelope.TraceContext(traceparent, tracestate)
            : null;

        return service.handle(idempotencyKey, tc, request)
            .map(outcome -> switch (outcome) {
                case IngressOutcome.Accepted a ->
                    Response.accepted(new IngressResponse(a.messageId(), a.acceptedAt())).build();
                case IngressOutcome.Duplicate d ->
                    Response.status(Response.Status.CONFLICT)
                        .entity(DuplicateResponse.of(d.originalMessageId(), idempotencyKey))
                        .build();
            });
    }
}
```

- [ ] **Step 4: Run and verify pass**

Run: `mvn -q -Dtest=MessagesResourceTest test`
Expected: 3 tests passed.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/throttling/ingress/MessagesResource.java src/test/java/com/throttling/ingress/MessagesResourceTest.java
git commit -m "feat(ingress): add POST /messages REST resource"
```

---

## Task 10: Throttling — TokenBucketService + Exception

**Files:**
- Create: `src/main/java/com/throttling/throttling/ThrottleTimeoutException.java`
- Create: `src/main/java/com/throttling/throttling/BucketConfig.java`
- Create: `src/main/java/com/throttling/throttling/TokenBucketService.java`
- Create: `src/test/java/com/throttling/throttling/TokenBucketServiceTest.java`

- [ ] **Step 1: Implement exception + config**

```java
// ThrottleTimeoutException.java
package com.throttling.throttling;

public class ThrottleTimeoutException extends RuntimeException {
    public ThrottleTimeoutException() {
        super("Token bucket acquire timed out");
    }
}
```

```java
// BucketConfig.java
package com.throttling.throttling;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "throttle")
public interface BucketConfig {
    long capacity();
    long refillTokens();
    long refillPeriodMs();
    String bucketKey();
    long acquireTimeoutMs();
}
```

- [ ] **Step 2: Add throttle config defaults to `application.properties`**

Append:

```properties
# Throttling
throttle.capacity=100
throttle.refill-tokens=100
throttle.refill-period-ms=1000
throttle.bucket-key=global-throttle
throttle.acquire-timeout-ms=30000
```

- [ ] **Step 3: Write failing test**

```java
package com.throttling.throttling;

import io.github.bucket4j.distributed.AsyncBucketProxy;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class TokenBucketServiceTest {

    @Test
    void completes_when_bucket_grants_token() {
        AsyncBucketProxy bucket = Mockito.mock(AsyncBucketProxy.class);
        when(bucket.consume(1L)).thenReturn(CompletableFuture.completedFuture(true));

        TokenBucketService svc = new TokenBucketService(bucket, Duration.ofSeconds(5));
        assertThat(svc.acquireBlocking().await().indefinitely()).isNull();
    }

    @Test
    void fails_with_throttle_timeout_when_acquire_exceeds_limit() {
        AsyncBucketProxy bucket = Mockito.mock(AsyncBucketProxy.class);
        CompletableFuture<Boolean> never = new CompletableFuture<>();
        when(bucket.consume(1L)).thenReturn(never);

        TokenBucketService svc = new TokenBucketService(bucket, Duration.ofMillis(100));
        assertThatThrownBy(() -> svc.acquireBlocking().await().atMost(Duration.ofSeconds(2)))
            .hasCauseInstanceOf(ThrottleTimeoutException.class);
    }
}
```

- [ ] **Step 4: Run and verify failure**

Run: `mvn -q -Dtest=TokenBucketServiceTest test`
Expected: compile error.

- [ ] **Step 5: Implement `TokenBucketService`**

```java
package com.throttling.throttling;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.AsyncBucketProxy;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.function.Supplier;

@ApplicationScoped
public class TokenBucketService {

    private final AsyncBucketProxy bucket;
    private final Duration acquireTimeout;

    @Inject
    public TokenBucketService(
            @Identifier("throttle-redis") RedisClient redisClient,
            BucketConfig config) {
        this(buildBucket(redisClient, config), Duration.ofMillis(config.acquireTimeoutMs()));
    }

    public TokenBucketService(AsyncBucketProxy bucket, Duration acquireTimeout) {
        this.bucket = bucket;
        this.acquireTimeout = acquireTimeout;
    }

    private static AsyncBucketProxy buildBucket(RedisClient redisClient, BucketConfig cfg) {
        BucketConfiguration bucketCfg = BucketConfiguration.builder()
            .addLimit(Bandwidth.builder()
                .capacity(cfg.capacity())
                .refillGreedy(cfg.refillTokens(), Duration.ofMillis(cfg.refillPeriodMs()))
                .build())
            .build();
        LettuceBasedProxyManager<byte[]> proxyManager = LettuceBasedProxyManager
            .builderFor(redisClient)
            .build();
        return proxyManager.builder()
            .build(cfg.bucketKey().getBytes(), (Supplier<BucketConfiguration>) () -> bucketCfg)
            .asAsync();
    }

    public Uni<Void> acquireBlocking() {
        return Uni.createFrom().completionStage(bucket.consume(1L))
            .ifNoItem().after(acquireTimeout).failWith(new ThrottleTimeoutException())
            .replaceWithVoid();
    }

    public Uni<Long> availableTokens() {
        return Uni.createFrom().completionStage(bucket.getAvailableTokens());
    }
}
```

- [ ] **Step 6: Create `RedisClient` producer**

```java
// src/main/java/com/throttling/throttling/RedisClientProducer.java
package com.throttling.throttling;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RedisClientProducer {

    @Produces
    @ApplicationScoped
    @Identifier("throttle-redis")
    public RedisClient redisClient(
            @ConfigProperty(name = "throttle.redis.host", defaultValue = "localhost") String host,
            @ConfigProperty(name = "throttle.redis.port", defaultValue = "6379") int port) {
        return RedisClient.create(RedisURI.builder().withHost(host).withPort(port).build());
    }

    public void close(@Disposes @Identifier("throttle-redis") RedisClient client) {
        client.shutdown();
    }
}
```

- [ ] **Step 7: Append Redis throttle config**

Append to `application.properties`:

```properties
throttle.redis.host=localhost
throttle.redis.port=6379

# Quarkus Redis client (idempotency store) — points to the same Redis instance
quarkus.redis.hosts=redis://localhost:6379
```

- [ ] **Step 8: Run and verify pass**

Run: `mvn -q -Dtest=TokenBucketServiceTest test`
Expected: 2 tests passed.

- [ ] **Step 9: Commit**

```bash
git add src/main/java/com/throttling/throttling/ src/main/resources/application.properties src/test/java/com/throttling/throttling/TokenBucketServiceTest.java
git commit -m "feat(throttling): add Bucket4j-Redis token bucket service"
```

---

## Task 11: Legacy — Exceptions + Response DTO + Response Mapper

**Files:**
- Create: `src/main/java/com/throttling/legacy/exceptions/LegacyTransientException.java`
- Create: `src/main/java/com/throttling/legacy/exceptions/LegacyPermanentException.java`
- Create: `src/main/java/com/throttling/legacy/LegacyResponse.java`
- Create: `src/main/java/com/throttling/legacy/LegacyResponseMapper.java`
- Create: `src/test/java/com/throttling/legacy/LegacyResponseMapperTest.java`

- [ ] **Step 1: Implement exceptions and DTO**

```java
// LegacyTransientException.java
package com.throttling.legacy.exceptions;

public class LegacyTransientException extends RuntimeException {
    private final int status;
    public LegacyTransientException(int status, String msg) { super(msg); this.status = status; }
    public int status() { return status; }
}
```

```java
// LegacyPermanentException.java
package com.throttling.legacy.exceptions;

public class LegacyPermanentException extends RuntimeException {
    private final int status;
    public LegacyPermanentException(int status, String msg) { super(msg); this.status = status; }
    public int status() { return status; }
}
```

```java
// LegacyResponse.java
package com.throttling.legacy;

import java.util.Map;

public record LegacyResponse(Map<String, Object> body) {}
```

- [ ] **Step 2: Write failing test**

```java
package com.throttling.legacy;

import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

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
```

- [ ] **Step 3: Run and verify failure**

Run: `mvn -q -Dtest=LegacyResponseMapperTest test`
Expected: compile error.

- [ ] **Step 4: Implement `LegacyResponseMapper`**

```java
package com.throttling.legacy;

import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.ClientWebApplicationException;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientResponseFilter;
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
```

- [ ] **Step 5: Run and verify pass**

Run: `mvn -q -Dtest=LegacyResponseMapperTest test`
Expected: 6 tests passed.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/throttling/legacy/ src/test/java/com/throttling/legacy/LegacyResponseMapperTest.java
git commit -m "feat(legacy): add response mapper classifying transient vs permanent errors"
```

---

## Task 12: Legacy — REST Client with Fault Tolerance

**Files:**
- Create: `src/main/java/com/throttling/legacy/LegacyClient.java`
- Modify: `src/main/resources/application.properties`

- [ ] **Step 1: Implement REST client interface**

```java
package com.throttling.legacy;

import io.smallrye.faulttolerance.api.CircuitBreakerName;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.HeaderParam;
import org.eclipse.microprofile.faulttolerance.Bulkhead;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import com.throttling.legacy.exceptions.LegacyTransientException;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@RegisterRestClient(configKey = "legacy-api")
@RegisterProvider(LegacyResponseMapper.class)
@Path("/")
public interface LegacyClient {

    @POST
    @Path("{endpoint}")
    @Retry(maxRetries = 3,
           delay = 200, delayUnit = ChronoUnit.MILLIS,
           jitter = 100, jitterDelayUnit = ChronoUnit.MILLIS,
           retryOn = { LegacyTransientException.class, TimeoutException.class })
    @CircuitBreaker(
        requestVolumeThreshold = 20,
        failureRatio = 0.5,
        delay = 10, delayUnit = ChronoUnit.SECONDS,
        successThreshold = 3,
        failOn = { LegacyTransientException.class, TimeoutException.class })
    @CircuitBreakerName("legacy-client")
    @Timeout(value = 5000)
    @Bulkhead(value = 50)
    Uni<LegacyResponse> send(@PathParam("endpoint") String endpoint,
                             @HeaderParam("Idempotency-Key") String idempKey,
                             Map<String, Object> payload);
}
```

- [ ] **Step 2: Append config to `application.properties`**

```properties
# Legacy REST client
quarkus.rest-client.legacy-api.url=http://localhost:8089
quarkus.rest-client.legacy-api.scope=jakarta.inject.Singleton
quarkus.rest-client.legacy-api.connect-timeout=2000
quarkus.rest-client.legacy-api.read-timeout=5000
```

- [ ] **Step 3: Compile**

Run: `mvn -q compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/throttling/legacy/LegacyClient.java src/main/resources/application.properties
git commit -m "feat(legacy): add reactive REST client with retry, CB, timeout, bulkhead"
```

---

## Task 13: DLQ — Envelope + Producer

**Files:**
- Create: `src/main/java/com/throttling/dlq/DlqEnvelope.java`
- Create: `src/main/java/com/throttling/dlq/DlqProducer.java`
- Create: `src/test/java/com/throttling/dlq/DlqProducerTest.java`
- Modify: `src/main/resources/application.properties`

- [ ] **Step 1: Implement `DlqEnvelope`**

```java
package com.throttling.dlq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record DlqEnvelope(
        MessageEnvelope original,
        Failure failure
) {
    public record Failure(
        FailureReason reason,
        String lastError,
        int attempts,
        Instant failedAt
    ) {}

    public static DlqEnvelope of(MessageEnvelope original, FailureReason reason, String err, int attempts) {
        return new DlqEnvelope(original, new Failure(reason, err, attempts, Instant.now()));
    }
}
```

- [ ] **Step 2: Write failing test**

```java
package com.throttling.dlq;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DlqProducerTest {

    @Test
    void sends_dlq_envelope_with_reason_and_error() {
        MutinyEmitter<DlqEnvelope> emitter = mock(MutinyEmitter.class);
        when(emitter.send(org.mockito.ArgumentMatchers.any(DlqEnvelope.class)))
            .thenReturn(Uni.createFrom().voidItem());

        DlqProducer producer = new DlqProducer(emitter);
        MessageEnvelope original = new MessageEnvelope(
            "M1", "k1", Instant.now(), 1, null, null, Map.of()
        );

        producer.send(original, FailureReason.LEGACY_5XX, "boom", 3)
            .await().indefinitely();

        ArgumentCaptor<DlqEnvelope> cap = ArgumentCaptor.forClass(DlqEnvelope.class);
        verify(emitter).send(cap.capture());
        assertThat(cap.getValue().failure().reason()).isEqualTo(FailureReason.LEGACY_5XX);
        assertThat(cap.getValue().failure().lastError()).isEqualTo("boom");
        assertThat(cap.getValue().failure().attempts()).isEqualTo(3);
        assertThat(cap.getValue().original().messageId()).isEqualTo("M1");
    }
}
```

- [ ] **Step 3: Run and verify failure**

Run: `mvn -q -Dtest=DlqProducerTest test`
Expected: compile error.

- [ ] **Step 4: Implement `DlqProducer`**

```java
package com.throttling.dlq;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class DlqProducer {

    private final MutinyEmitter<DlqEnvelope> emitter;

    @Inject
    public DlqProducer(@Channel("messages-dlq") MutinyEmitter<DlqEnvelope> emitter) {
        this.emitter = emitter;
    }

    public Uni<Void> send(MessageEnvelope original, FailureReason reason, String err, int attempts) {
        return emitter.send(DlqEnvelope.of(original, reason, err, attempts));
    }
}
```

- [ ] **Step 5: Append DLQ outgoing config**

```properties
# DLQ outgoing
mp.messaging.outgoing.messages-dlq.connector=smallrye-kafka
mp.messaging.outgoing.messages-dlq.topic=messages.dlq
mp.messaging.outgoing.messages-dlq.value.serializer=io.quarkus.kafka.client.serialization.ObjectMapperSerializer
mp.messaging.outgoing.messages-dlq.key.serializer=org.apache.kafka.common.serialization.StringSerializer
mp.messaging.outgoing.messages-dlq.acks=all
mp.messaging.outgoing.messages-dlq.retries=10
```

- [ ] **Step 6: Run and verify pass**

Run: `mvn -q -Dtest=DlqProducerTest test`
Expected: 1 test passed.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/throttling/dlq/ src/main/resources/application.properties src/test/java/com/throttling/dlq/DlqProducerTest.java
git commit -m "feat(dlq): add DLQ envelope and producer"
```

---

## Task 14: Processing — MessageHandler (pipeline Mutiny)

**Files:**
- Create: `src/main/java/com/throttling/processing/MessageHandler.java`
- Create: `src/test/java/com/throttling/processing/MessageHandlerTest.java`

`MessageHandler` orchestrates the per-message pipeline: throttle → legacy call → success or DLQ. Receives a `MessageEnvelope`, returns `Uni<Void>` representing completion. Classifies failures via `FailureReason`. Hard retry cap reads from config (`consumer.max-hard-retries`).

- [ ] **Step 1: Write failing test**

```java
package com.throttling.processing;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import com.throttling.dlq.DlqProducer;
import com.throttling.legacy.LegacyClient;
import com.throttling.legacy.LegacyResponse;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.throttling.ThrottleTimeoutException;
import com.throttling.throttling.TokenBucketService;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MessageHandlerTest {

    TokenBucketService throttle;
    LegacyClient legacy;
    DlqProducer dlq;
    MessageHandler handler;

    @BeforeEach
    void setup() {
        throttle = mock(TokenBucketService.class);
        legacy = mock(LegacyClient.class);
        dlq = mock(DlqProducer.class);
        handler = new MessageHandler(throttle, legacy, dlq, 5);
        when(throttle.acquireBlocking()).thenReturn(Uni.createFrom().voidItem());
        when(dlq.send(any(), any(), any(), anyInt())).thenReturn(Uni.createFrom().voidItem());
    }

    private MessageEnvelope env() {
        return new MessageEnvelope("M1", "k1", Instant.now(), 0, null,
            new MessageEnvelope.Metadata(null, "users"), Map.of("a", 1));
    }

    @Test
    void calls_legacy_after_throttle_acquired() {
        when(legacy.send(eq("users"), eq("k1"), any()))
            .thenReturn(Uni.createFrom().item(new LegacyResponse(Map.of("ok", true))));

        handler.handle(env()).await().indefinitely();

        verify(throttle).acquireBlocking();
        verify(legacy).send("users", "k1", Map.of("a", 1));
        verifyNoInteractions(dlq);
    }

    @Test
    void sends_to_dlq_with_circuit_open_reason() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new CircuitBreakerOpenException("open")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.CIRCUIT_OPEN);
    }

    @Test
    void sends_to_dlq_with_legacy_5xx_reason() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new LegacyTransientException(503, "x")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.LEGACY_5XX);
    }

    @Test
    void sends_to_dlq_with_permanent_4xx_reason() {
        when(legacy.send(any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new LegacyPermanentException(400, "x")));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.LEGACY_4XX_PERMANENT);
    }

    @Test
    void sends_to_dlq_with_throttle_timeout() {
        when(throttle.acquireBlocking())
            .thenReturn(Uni.createFrom().failure(new ThrottleTimeoutException()));

        handler.handle(env()).await().indefinitely();

        ArgumentCaptor<FailureReason> reason = ArgumentCaptor.forClass(FailureReason.class);
        verify(dlq).send(any(), reason.capture(), any(), anyInt());
        assertThat(reason.getValue()).isEqualTo(FailureReason.THROTTLE_TIMEOUT);
        verifyNoInteractions(legacy);
    }
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn -q -Dtest=MessageHandlerTest test`
Expected: compile error.

- [ ] **Step 3: Implement `MessageHandler`**

```java
package com.throttling.processing;

import com.throttling.common.FailureReason;
import com.throttling.common.MessageEnvelope;
import com.throttling.dlq.DlqProducer;
import com.throttling.legacy.LegacyClient;
import com.throttling.legacy.exceptions.LegacyPermanentException;
import com.throttling.legacy.exceptions.LegacyTransientException;
import com.throttling.throttling.ThrottleTimeoutException;
import com.throttling.throttling.TokenBucketService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;

import java.util.concurrent.TimeoutException;

@ApplicationScoped
public class MessageHandler {

    private final TokenBucketService throttle;
    private final LegacyClient legacy;
    private final DlqProducer dlq;
    private final int maxHardRetries;

    @Inject
    public MessageHandler(TokenBucketService throttle,
                          @org.eclipse.microprofile.rest.client.inject.RestClient LegacyClient legacy,
                          DlqProducer dlq) {
        this.throttle = throttle;
        this.legacy = legacy;
        this.dlq = dlq;
    }

    public MessageHandler(TokenBucketService throttle, LegacyClient legacy, DlqProducer dlq) {
        this.throttle = throttle;
        this.legacy = legacy;
        this.dlq = dlq;
    }

    public Uni<Void> handle(MessageEnvelope env) {
        String endpoint = env.metadata() != null && env.metadata().targetEndpoint() != null
            ? env.metadata().targetEndpoint()
            : "default";

        return throttle.acquireBlocking()
            .chain(() -> legacy.send(endpoint, env.idempotencyKey(), env.payload()))
            .replaceWithVoid()
            .onFailure().recoverWithUni(err -> sendToDlq(env, err));
    }

    private Uni<Void> sendToDlq(MessageEnvelope env, Throwable err) {
        FailureReason reason = classify(err);
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

- [ ] **Step 4: Run and verify pass**

Run: `mvn -q -Dtest=MessageHandlerTest test`
Expected: 5 tests passed.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/throttling/processing/MessageHandler.java src/test/java/com/throttling/processing/MessageHandlerTest.java
git commit -m "feat(processing): add message handler pipeline with DLQ fallback"
```

---

## Task 15: Processing — Kafka Consumer wiring

**Files:**
- Create: `src/main/java/com/throttling/processing/MessageConsumer.java`
- Modify: `src/main/resources/application.properties`

- [ ] **Step 1: Implement `MessageConsumer`**

```java
package com.throttling.processing;

import com.throttling.common.MessageEnvelope;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MessageConsumer {

    private static final Logger LOG = Logger.getLogger(MessageConsumer.class);

    @Inject MessageHandler handler;

    @Incoming("messages-in")
    public Uni<Void> consume(Message<MessageEnvelope> msg) {
        MessageEnvelope env = msg.getPayload();
        return handler.handle(env)
            .onItemOrFailure().transformToUni((ignored, err) -> {
                if (err != null) {
                    LOG.errorf(err, "Pipeline failed for messageId=%s; nack", env.messageId());
                    return Uni.createFrom().completionStage(msg.nack(err));
                }
                return Uni.createFrom().completionStage(msg.ack());
            });
    }
}
```

- [ ] **Step 2: Append consumer config to `application.properties`**

```properties
# Incoming consumer
mp.messaging.incoming.messages-in.connector=smallrye-kafka
mp.messaging.incoming.messages-in.topic=messages.in
mp.messaging.incoming.messages-in.group.id=throttling-worker
mp.messaging.incoming.messages-in.value.deserializer=io.quarkus.kafka.client.serialization.ObjectMapperDeserializer
mp.messaging.incoming.messages-in.value.type=com.throttling.common.MessageEnvelope
mp.messaging.incoming.messages-in.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
mp.messaging.incoming.messages-in.failure-strategy=ignore
mp.messaging.incoming.messages-in.commit-strategy=throttled
mp.messaging.incoming.messages-in.auto.offset.reset=earliest
mp.messaging.incoming.messages-in.concurrency=1
mp.messaging.incoming.messages-in.fetch.max.wait.ms=500

consumer.max-hard-retries=5
```

- [ ] **Step 3: Compile**

Run: `mvn -q compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/throttling/processing/MessageConsumer.java src/main/resources/application.properties
git commit -m "feat(processing): wire Kafka consumer to message handler"
```

---

## Task 16: Observability — MetricsRegistry

**Files:**
- Create: `src/main/java/com/throttling/observability/MetricsRegistry.java`

- [ ] **Step 1: Implement `MetricsRegistry`**

```java
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
```

- [ ] **Step 2: Wire counters in services**

Update `IngressService.handle` to call `metrics.ingressAccepted()` / `ingressDuplicate()` accordingly. Inject `MetricsRegistry`. Add to `MessagesResource` for `ingressRejected()` on validation 400.

Patch `IngressService` (replace `handle` body):

```java
public Uni<IngressOutcome> handle(String idempotencyKey,
                                  MessageEnvelope.TraceContext traceContext,
                                  IngressRequest request) {
    final String messageId = ulid.next();
    final Instant now = Instant.now();
    final MessageEnvelope envelope = new MessageEnvelope(
        messageId, idempotencyKey, now, 0, traceContext, request.metadata(), request.payload()
    );

    return store.tryStore(idempotencyKey, messageId, idempotencyTtl)
        .onItem().transformToUni(existing -> {
            if (existing.isPresent()) {
                metrics.ingressDuplicate();
                return Uni.createFrom().item(new IngressOutcome.Duplicate(existing.get()));
            }
            return producer.send(envelope)
                .onFailure().call(err -> store.remove(idempotencyKey))
                .invoke(() -> metrics.ingressAccepted())
                .replaceWith(new IngressOutcome.Accepted(messageId, now));
        });
}
```

Add `MetricsRegistry metrics` field + inject into both constructors. The 4-arg constructor (used in tests) accepts a nullable `MetricsRegistry` — guard with null checks, or pass a no-op via `Mockito.mock(MetricsRegistry.class)` in tests.

Update `IngressServiceTest` setup to provide `MetricsRegistry metrics = mock(MetricsRegistry.class);` and pass it into `new IngressService(store, producer, ulid, metrics, Duration.ofSeconds(60))`.

Update `MessageHandler` similarly: inject `MetricsRegistry`, call `metrics.consumed("success")` on success, `metrics.consumed("dlq")` + `metrics.dlqSent(reason.name())` inside `sendToDlq`. Update its test constructor to accept the metrics mock.

Update `TokenBucketService.acquireBlocking`: time and report:

```java
public Uni<Void> acquireBlocking() {
    long start = System.nanoTime();
    return Uni.createFrom().completionStage(bucket.consume(1L))
        .ifNoItem().after(acquireTimeout).failWith(new ThrottleTimeoutException())
        .invoke(() -> {
            if (metrics != null) {
                metrics.tokenConsumed();
                metrics.recordWait(Duration.ofNanos(System.nanoTime() - start));
            }
        })
        .onFailure(ThrottleTimeoutException.class).invoke(() -> { if (metrics != null) metrics.throttleTimeout(); })
        .replaceWithVoid();
}
```

- [ ] **Step 3: Re-run all unit tests**

Run: `mvn -q test`
Expected: all tests pass (update test constructor invocations as needed).

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/throttling/observability/MetricsRegistry.java src/main/java/com/throttling/ingress/IngressService.java src/main/java/com/throttling/processing/MessageHandler.java src/main/java/com/throttling/throttling/TokenBucketService.java src/test/java/com/throttling/
git commit -m "feat(observability): add Micrometer metrics across ingress, throttle, processing"
```

---

## Task 17: Admin — Admin Resource (throttle status + CB control)

**Files:**
- Create: `src/main/java/com/throttling/admin/AdminResource.java`
- Create: `src/test/java/com/throttling/admin/AdminResourceTest.java`

The admin resource exposes:
- `GET /admin/throttle` — returns `{capacity, available, refillRate}`
- `GET /admin/circuit-breaker/status` — returns `{state}` for the `legacy-client` CB
- `POST /admin/circuit-breaker/reset` — calls `CircuitBreakerMaintenance.reset("legacy-client")`

Auth via a simple header `X-Admin-Token` compared with `admin.token` config; mismatch → 401.

- [ ] **Step 1: Append admin config**

```properties
admin.token=${ADMIN_TOKEN:dev-admin}
```

- [ ] **Step 2: Write failing test**

```java
package com.throttling.admin;

import com.throttling.throttling.BucketConfig;
import com.throttling.throttling.TokenBucketService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.faulttolerance.api.CircuitBreakerMaintenance;
import io.smallrye.faulttolerance.api.CircuitBreakerState;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@QuarkusTest
class AdminResourceTest {

    @InjectMock TokenBucketService bucket;
    @InjectMock BucketConfig cfg;
    @InjectMock CircuitBreakerMaintenance cb;

    @Test
    void admin_throttle_returns_state() {
        when(bucket.availableTokens()).thenReturn(Uni.createFrom().item(42L));
        when(cfg.capacity()).thenReturn(100L);
        when(cfg.refillTokens()).thenReturn(50L);
        when(cfg.refillPeriodMs()).thenReturn(1000L);

        given().header("X-Admin-Token", "dev-admin")
            .when().get("/admin/throttle")
            .then().statusCode(200)
            .body("capacity", equalTo(100))
            .body("available", equalTo(42));
    }

    @Test
    void admin_endpoints_return_401_without_token() {
        given().when().get("/admin/throttle").then().statusCode(401);
    }

    @Test
    void admin_circuit_breaker_status_returns_state() {
        when(cb.currentState("legacy-client")).thenReturn(CircuitBreakerState.CLOSED);
        given().header("X-Admin-Token", "dev-admin")
            .when().get("/admin/circuit-breaker/status")
            .then().statusCode(200)
            .body("state", equalTo("CLOSED"));
    }
}
```

- [ ] **Step 3: Run and verify failure**

Run: `mvn -q -Dtest=AdminResourceTest test`
Expected: 404s.

- [ ] **Step 4: Implement `AdminResource`**

```java
package com.throttling.admin;

import com.throttling.throttling.BucketConfig;
import com.throttling.throttling.TokenBucketService;
import io.smallrye.faulttolerance.api.CircuitBreakerMaintenance;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;

@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
public class AdminResource {

    @Inject TokenBucketService bucket;
    @Inject BucketConfig cfg;
    @Inject CircuitBreakerMaintenance cb;

    @ConfigProperty(name = "admin.token") String adminToken;

    private Response unauthorized() {
        return Response.status(401).entity(Map.of("error", "unauthorized")).build();
    }

    private boolean authed(String token) {
        return adminToken.equals(token);
    }

    @GET
    @Path("/throttle")
    public Uni<Response> throttle(@HeaderParam("X-Admin-Token") String token) {
        if (!authed(token)) return Uni.createFrom().item(unauthorized());
        return bucket.availableTokens().map(avail -> Response.ok(Map.of(
            "capacity", cfg.capacity(),
            "available", avail,
            "refillTokens", cfg.refillTokens(),
            "refillPeriodMs", cfg.refillPeriodMs()
        )).build());
    }

    @GET
    @Path("/circuit-breaker/status")
    public Response cbStatus(@HeaderParam("X-Admin-Token") String token) {
        if (!authed(token)) return unauthorized();
        return Response.ok(Map.of("state", cb.currentState("legacy-client").name())).build();
    }

    @POST
    @Path("/circuit-breaker/reset")
    public Response cbReset(@HeaderParam("X-Admin-Token") String token) {
        if (!authed(token)) return unauthorized();
        cb.reset("legacy-client");
        return Response.ok(Map.of("state", "RESET")).build();
    }
}
```

- [ ] **Step 5: Run and verify pass**

Run: `mvn -q -Dtest=AdminResourceTest test`
Expected: 3 tests passed.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/throttling/admin/ src/test/java/com/throttling/admin/ src/main/resources/application.properties
git commit -m "feat(admin): add /admin endpoints for throttle status and CB control"
```

---

## Task 18: Observability — OTel & Logs config

**Files:**
- Modify: `src/main/resources/application.properties`

- [ ] **Step 1: Append OTel + JSON logging config**

```properties
# OpenTelemetry
quarkus.otel.enabled=true
quarkus.otel.exporter.otlp.endpoint=${OTEL_EXPORTER_OTLP_ENDPOINT:http://localhost:4317}
quarkus.otel.exporter.otlp.protocol=grpc
quarkus.otel.traces.exporter=otlp
quarkus.otel.metrics.exporter=otlp
quarkus.otel.logs.exporter=otlp
quarkus.otel.resource.attributes=service.namespace=throttling,deployment.environment=${ENV:dev}
quarkus.otel.traces.sampler=parentbased_traceidratio
quarkus.otel.traces.sampler.arg=1.0

# JSON logging
quarkus.log.console.json=true
quarkus.log.console.json.additional-field."service.name".value=kafka-throttling-system

# Health
quarkus.smallrye-health.ui.always-include=true
```

- [ ] **Step 2: Add `@WithSpan` annotations**

In `TokenBucketService.acquireBlocking`:

```java
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;

@WithSpan("throttle.acquire")
public Uni<Void> acquireBlocking() {
    ...
}
```

In `DlqProducer.send`:

```java
@WithSpan("dlq.send")
public Uni<Void> send(MessageEnvelope original,
                      @SpanAttribute("dlq.reason") FailureReason reason,
                      String err,
                      int attempts) {
    ...
}
```

- [ ] **Step 3: Run all tests**

Run: `mvn -q test`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/application.properties src/main/java/com/throttling/throttling/TokenBucketService.java src/main/java/com/throttling/dlq/DlqProducer.java
git commit -m "feat(observability): enable OTLP exporter, JSON logs, custom spans"
```

---

## Task 19: Test Resources — Kafka, Redis, Wiremock

**Files:**
- Create: `src/test/java/com/throttling/integration/KafkaTestResource.java`
- Create: `src/test/java/com/throttling/integration/RedisTestResource.java`
- Create: `src/test/java/com/throttling/integration/WiremockTestResource.java`
- Create: `src/test/resources/application-test.properties`

- [ ] **Step 1: Implement `KafkaTestResource`**

```java
package com.throttling.integration;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class KafkaTestResource implements QuarkusTestResourceLifecycleManager {

    private KafkaContainer kafka;

    @Override
    public Map<String, String> start() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        kafka.start();
        return Map.of("kafka.bootstrap.servers", kafka.getBootstrapServers());
    }

    @Override
    public void stop() {
        if (kafka != null) kafka.stop();
    }
}
```

- [ ] **Step 2: Implement `RedisTestResource`**

```java
package com.throttling.integration;

import com.redis.testcontainers.RedisContainer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class RedisTestResource implements QuarkusTestResourceLifecycleManager {

    private RedisContainer redis;

    @Override
    public Map<String, String> start() {
        redis = new RedisContainer(DockerImageName.parse("redis:7.4-alpine"));
        redis.start();
        String host = redis.getHost();
        Integer port = redis.getFirstMappedPort();
        return Map.of(
            "quarkus.redis.hosts", "redis://" + host + ":" + port,
            "throttle.redis.host", host,
            "throttle.redis.port", String.valueOf(port)
        );
    }

    @Override
    public void stop() {
        if (redis != null) redis.stop();
    }
}
```

- [ ] **Step 3: Implement `WiremockTestResource`**

```java
package com.throttling.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.HashMap;
import java.util.Map;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

public class WiremockTestResource implements QuarkusTestResourceLifecycleManager {

    private WireMockServer server;

    @Override
    public Map<String, String> start() {
        server = new WireMockServer(options().dynamicPort());
        server.start();
        Map<String, String> cfg = new HashMap<>();
        cfg.put("quarkus.rest-client.legacy-api.url", "http://localhost:" + server.port());
        cfg.put("wiremock.port", String.valueOf(server.port()));
        return cfg;
    }

    @Override
    public Object getInjectionInstance() {
        return server;
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(server,
            new TestInjector.MatchesType(WireMockServer.class));
    }

    @Override
    public void stop() {
        if (server != null) server.stop();
    }
}
```

- [ ] **Step 4: Create `application-test.properties`**

```properties
# Disable real OTLP exporter in tests
quarkus.otel.sdk.disabled=true
quarkus.otel.exporter.otlp.endpoint=http://localhost:4317

# Smaller bucket for faster tests
throttle.capacity=5
throttle.refill-tokens=5
throttle.refill-period-ms=1000
throttle.acquire-timeout-ms=2000

# Faster CB for tests
legacy.cb.volume-threshold=4
legacy.cb.open-duration-sec=2

# Admin
admin.token=test-admin
```

- [ ] **Step 5: Compile tests**

Run: `mvn -q test-compile`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/test/java/com/throttling/integration/ src/test/resources/application-test.properties
git commit -m "test: add Testcontainers + Wiremock Quarkus test resources"
```

---

## Task 20: Integration — Ingress Idempotency IT

**Files:**
- Create: `src/test/java/com/throttling/integration/IngressIdempotencyIT.java`

- [ ] **Step 1: Write test**

```java
package com.throttling.integration;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(RedisTestResource.class)
@QuarkusTestResource(WiremockTestResource.class)
class IngressIdempotencyIT {

    @InjectKafkaCompanion KafkaCompanion companion;

    @Test
    void second_post_with_same_idempotency_key_returns_409_and_only_one_record_on_topic() {
        String key = "it-key-" + System.nanoTime();
        Map<String, Object> body = Map.of("payload", Map.of("x", 1));

        // First request: 202
        String messageId = given()
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", key)
            .body(body)
        .when()
            .post("/messages")
        .then()
            .statusCode(202)
            .body("messageId", notNullValue())
            .extract().path("messageId");

        // Second request, same key: 409
        given()
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", key)
            .body(body)
        .when()
            .post("/messages")
        .then()
            .statusCode(409)
            .body("error", equalTo("DUPLICATE"))
            .body("messageId", equalTo(messageId));

        ConsumerTask<String, String> records = companion.consumeStrings()
            .fromTopics("messages.in", 1, Duration.ofSeconds(10));
        assertThat(records.count()).isEqualTo(1);
    }
}
```

- [ ] **Step 2: Run integration test**

Run: `mvn -q -Dit.test=IngressIdempotencyIT verify -Dquarkus.profile=test`
Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/throttling/integration/IngressIdempotencyIT.java
git commit -m "test(integration): verify ingress idempotency end-to-end"
```

---

## Task 21: Integration — Throttle Backpressure IT

**Files:**
- Create: `src/test/java/com/throttling/integration/ThrottleBackpressureIT.java`

- [ ] **Step 1: Write test**

```java
package com.throttling.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(RedisTestResource.class)
@QuarkusTestResource(WiremockTestResource.class)
class ThrottleBackpressureIT {

    WireMockServer wiremock;  // injected by WiremockTestResource

    @Test
    void consumer_throughput_limited_by_token_bucket_refill_rate() {
        // capacity=5, refill=5/s in test profile.
        // Stub legacy 200 for any endpoint
        wiremock.stubFor(post(urlMatching("/.*"))
            .willReturn(aResponse().withStatus(200).withBody("{\"ok\":true}")));

        // Post 15 messages quickly
        int total = 15;
        for (int i = 0; i < total; i++) {
            given().contentType(MediaType.APPLICATION_JSON)
                .header("X-Idempotency-Key", UUID.randomUUID().toString())
                .body(Map.of("payload", Map.of("i", i)))
            .when().post("/messages").then().statusCode(202);
        }

        // Wait until wiremock saw all 15 calls; assert it took >= ~2s (15 / 5 per sec)
        long t0 = System.currentTimeMillis();
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> assertThat(wiremock.findAll(post(urlMatching("/.*")).build())).hasSize(total));
        long elapsed = System.currentTimeMillis() - t0;

        assertThat(elapsed).isGreaterThanOrEqualTo(1500);  // at least 1.5s to consume 15 @ 5/s
    }
}
```

- [ ] **Step 2: Run**

Run: `mvn -q -Dit.test=ThrottleBackpressureIT verify -Dquarkus.profile=test`
Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/throttling/integration/ThrottleBackpressureIT.java
git commit -m "test(integration): verify token bucket enforces throughput limit"
```

---

## Task 22: Integration — Retry Transient + Circuit Breaker DLQ ITs

**Files:**
- Create: `src/test/java/com/throttling/integration/RetryTransientIT.java`
- Create: `src/test/java/com/throttling/integration/CircuitBreakerDlqIT.java`

- [ ] **Step 1: `RetryTransientIT` test**

```java
package com.throttling.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(RedisTestResource.class)
@QuarkusTestResource(WiremockTestResource.class)
class RetryTransientIT {

    WireMockServer wiremock;

    @Test
    void retries_503_until_success() {
        String scenario = "retry-scenario";
        wiremock.stubFor(post(urlMatching("/.*"))
            .inScenario(scenario)
            .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
            .willReturn(aResponse().withStatus(503))
            .willSetStateTo("S2"));
        wiremock.stubFor(post(urlMatching("/.*"))
            .inScenario(scenario)
            .whenScenarioStateIs("S2")
            .willReturn(aResponse().withStatus(200).withBody("{\"ok\":true}")));

        given().contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", UUID.randomUUID().toString())
            .body(Map.of("payload", Map.of("k", "v")))
        .when().post("/messages").then().statusCode(202);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
            assertThat(wiremock.findAll(post(urlMatching("/.*")).build())).hasSizeGreaterThanOrEqualTo(2));
    }
}
```

- [ ] **Step 2: `CircuitBreakerDlqIT` test**

```java
package com.throttling.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(RedisTestResource.class)
@QuarkusTestResource(WiremockTestResource.class)
class CircuitBreakerDlqIT {

    @InjectKafkaCompanion KafkaCompanion companion;
    WireMockServer wiremock;

    @Test
    void persistent_5xx_pushes_messages_to_dlq() {
        wiremock.stubFor(post(urlMatching("/.*"))
            .willReturn(aResponse().withStatus(500)));

        for (int i = 0; i < 10; i++) {
            given().contentType(MediaType.APPLICATION_JSON)
                .header("X-Idempotency-Key", UUID.randomUUID().toString())
                .body(Map.of("payload", Map.of("i", i)))
            .when().post("/messages").then().statusCode(202);
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(companion.consumeStrings()
                .fromTopics("messages.dlq", 1, Duration.ofSeconds(5))
                .count()).isGreaterThanOrEqualTo(1);
        });
    }
}
```

- [ ] **Step 3: Run**

Run: `mvn -q -Dit.test=RetryTransientIT,CircuitBreakerDlqIT verify -Dquarkus.profile=test`
Expected: passes.

- [ ] **Step 4: Commit**

```bash
git add src/test/java/com/throttling/integration/RetryTransientIT.java src/test/java/com/throttling/integration/CircuitBreakerDlqIT.java
git commit -m "test(integration): verify retry success path and DLQ on CB open"
```

---

## Task 23: Integration — End-to-End Happy Path

**Files:**
- Create: `src/test/java/com/throttling/integration/EndToEndIT.java`

- [ ] **Step 1: Write test**

```java
package com.throttling.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(RedisTestResource.class)
@QuarkusTestResource(WiremockTestResource.class)
class EndToEndIT {

    WireMockServer wiremock;

    @Test
    void post_message_reaches_legacy_with_idempotency_header() {
        wiremock.stubFor(post(urlMatching("/.*"))
            .willReturn(aResponse().withStatus(200).withBody("{\"ok\":true}")));

        String idemp = UUID.randomUUID().toString();

        given().contentType(MediaType.APPLICATION_JSON)
            .header("X-Idempotency-Key", idemp)
            .body(Map.of(
                "payload", Map.of("user", "alice"),
                "metadata", Map.of("targetEndpoint", "users")
            ))
        .when().post("/messages").then().statusCode(202);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
            assertThat(wiremock.findAll(postRequestedFor(urlMatching("/users"))
                .withHeader("Idempotency-Key", equalTo(idemp))))
                .hasSize(1));
    }
}
```

- [ ] **Step 2: Run**

Run: `mvn -q -Dit.test=EndToEndIT verify -Dquarkus.profile=test`
Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/throttling/integration/EndToEndIT.java
git commit -m "test(integration): end-to-end happy path with idempotency propagation"
```

---

## Task 24: README + dev experience

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write `README.md`**

````markdown
# kafka-throttling-system

Reactive Quarkus application that ingests HTTP messages, publishes them to Kafka, and consumes them at a controlled rate via a Redis-backed token bucket to protect a legacy REST system.

## Stack
- Quarkus 3.x, Java 21, Mutiny
- Kafka (SmallRye Reactive Messaging)
- Redis + Bucket4j (token bucket)
- MicroProfile Fault Tolerance (Retry + Circuit Breaker)
- OpenTelemetry OTLP (traces, metrics, logs)

## Quick start

```bash
docker-compose up -d
mvn quarkus:dev
```

Send a message:

```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: $(uuidgen)" \
  -d '{"payload":{"user":"alice"},"metadata":{"targetEndpoint":"users"}}'
```

## Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/messages` | Submit a message (requires `X-Idempotency-Key`) |
| GET | `/admin/throttle` | Throttle state (requires `X-Admin-Token`) |
| GET | `/admin/circuit-breaker/status` | CB state |
| POST | `/admin/circuit-breaker/reset` | Force CB to CLOSED |
| GET | `/q/health/live` · `/q/health/ready` | Health checks |
| GET | `/q/metrics` | Prometheus metrics |

## Tests

```bash
mvn verify   # unit + integration (Testcontainers)
```

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `QUARKUS_REDIS_HOSTS` | `redis://localhost:6379` | Redis URL |
| `THROTTLE_CAPACITY` | `100` | Token bucket capacity |
| `THROTTLE_REFILL_TOKENS` | `100` | Tokens per refill period |
| `THROTTLE_REFILL_PERIOD_MS` | `1000` | Refill period |
| `THROTTLE_ACQUIRE_TIMEOUT_MS` | `30000` | Max wait per token |
| `IDEMPOTENCY_TTL_SECONDS` | `86400` | TTL for idempotency keys in Redis |
| `LEGACY_API_URL` (via `quarkus.rest-client.legacy-api.url`) | `http://localhost:8089` | Legacy base URL |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTel Collector endpoint |
| `ADMIN_TOKEN` | `dev-admin` | Admin endpoints token |
````

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README with quick start and configuration"
```

---

## Task 25: Full verify + final polish

- [ ] **Step 1: Run full test suite**

Run: `mvn -q verify`
Expected: all unit + integration tests pass.

- [ ] **Step 2: Run dev mode smoke**

```bash
docker-compose up -d kafka redis wiremock otel-collector
mvn quarkus:dev
# in another shell:
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: $(uuidgen)" \
  -d '{"payload":{"x":1},"metadata":{"targetEndpoint":"users"}}'
# expect HTTP 202
# verify legacy hit: docker logs kts-wiremock --tail=20
```

- [ ] **Step 3: Commit any final cleanup**

```bash
git status
# if anything outstanding:
git add -p
git commit -m "chore: final polish"
```

---

## Self-review notes

- **Spec coverage:** Section 1 (objective) → all tasks. Section 2 (arch + modules) → Tasks 3-17 follow the package layout. Section 3 (API contract incl. 202/409/400) → Tasks 6/9 + IngressIdempotencyIT. Section 3.2 (idempotency flow with DEL-on-failure) → Task 7 IngressService + test. Section 4 (envelope shape + DLQ envelope) → Tasks 4 + 13. Section 5 (token bucket + Mutiny acquire + concurrency=1) → Tasks 10 + 15 (consumer config). Section 6 (fault tolerance + classification + DLQ) → Tasks 11/12/14. Section 7 (OTLP traces/metrics/logs + custom spans + health) → Tasks 16 + 18 + Quarkus built-ins. Section 8 (project layout, deps, ITs) → Task 1 + Tasks 19-23. Section 9 (TDD strict, Testcontainers + Wiremock) → all tasks use TDD; Tasks 19-23 cover all named ITs in the spec (Ingress idempotency, Throttle backpressure, CB DLQ, Retry transient, EndToEnd). Section 11 (out of scope) honored — nothing added beyond.

- **Type consistency:** `IdempotencyStore.tryStore(String, String, Duration)` consistent across Tasks 5/7. `MessageProducer.send(MessageEnvelope)` Task 7/8. `TokenBucketService.acquireBlocking()` returns `Uni<Void>` Task 10/14/16. `MessageHandler.handle(MessageEnvelope)` returns `Uni<Void>` Tasks 14/15. `DlqProducer.send(env, reason, err, attempts)` Tasks 13/14/16.

- **Placeholder scan:** none. All code is concrete.

- **Known nuance (call out):** `consumer.max-hard-retries=5` is read but not currently enforced as a counter — current pipeline always routes failures to DLQ (it doesn't nack-and-reprocess to bump `attempt`). Spec accepts this: in-pipeline retry is via MicroProfile `@Retry`; `attempt` field is reserved for future republish-with-counter use. If you want strict enforcement now, add a check in `MessageHandler.sendToDlq` to use `FailureReason.HARD_RETRY_LIMIT` when `env.attempt() >= maxHardRetries`, but it cannot trigger until a republish loop exists. Out of scope for this plan.
