# Kafka Throttling System — Design Spec

**Data:** 2026-05-25
**Status:** Aprovado
**Stack alvo:** Quarkus 3.x (latest stable) · Java 21 · Mutiny · Kafka · Redis · OpenTelemetry

---

## 1. Objetivo

Construir uma aplicação Quarkus reativa (`kafka-throttling-system`) que:

1. Recebe mensagens via HTTP REST.
2. Garante idempotência na entrada usando Redis.
3. Publica mensagens em um tópico Kafka.
4. Consome essas mensagens em um ritmo controlado por um token bucket distribuído (Redis + Bucket4j).
5. Chama um sistema legado via HTTP REST com retry, circuit breaker e DLQ.
6. Emite observabilidade completa via OpenTelemetry OTLP (traces, metrics, logs).

O objetivo do throttling é proteger o sistema legado de sobrecarga, mantendo throughput previsível e backpressure natural.

---

## 2. Arquitetura geral

```
Cliente HTTP
   │  POST /messages
   ▼
┌───────────────────────────────────────────────────────────┐
│  kafka-throttling-system (Quarkus, JVM, Java 21)          │
│                                                           │
│  ingress/  ── Redis SET NX (idempotency) ── produce Kafka │
│                                                           │
│                              ▼                            │
│                       topic: messages.in                  │
│                              │                            │
│                              ▼                            │
│  processing/  ── throttling/ (Bucket4j-Redis) ──┐         │
│                                                 │         │
│                                                 ▼         │
│                              legacy/ (REST Client reativo)│
│                              + Fault Tolerance            │
│                                                 │         │
│                                                 ▼         │
│                                          topic: messages.dlq
│                                                           │
│  observability/ ── OTLP → OpenTelemetry Collector         │
└───────────────────────────────────────────────────────────┘
```

### 2.1 Decisão arquitetural

**Escolhido: Monolito modular** (1 deployable, fronteiras claras por pacote).

Pacotes Java (`com.throttling.*`):

| Pacote | Responsabilidade |
|---|---|
| `ingress` | REST resource, validação, idempotency check, produce Kafka |
| `processing` | Consumer reativo Kafka, orquestração handler |
| `throttling` | Wrapper Bucket4j-Redis exposto como API Mutiny |
| `legacy` | REST Client reativo + Fault Tolerance |
| `dlq` | Producer DLQ + envelope DLQ |
| `observability` | Configuração OTel, métricas custom, propagação trace |
| `admin` | Endpoints administrativos (CB reset, health throttle) |
| `common` | Envelope, ULID generator, exceções, constantes |

### 2.2 Alternativas consideradas

- **Monolito simples (pacotes flat)** — rejeitado por baixar disciplina de fronteira.
- **Split ingress/worker** — rejeitado por overhead de 2 pipelines CI/CD sem necessidade atual. Modularidade atual permite migração futura.

---

## 3. Contrato da API

### 3.1 POST `/messages`

**Request:**

```http
POST /messages
Content-Type: application/json
X-Idempotency-Key: <string obrigatória, ex: UUID>
traceparent: <W3C trace context, opcional>

{
  "payload": { ... JSON arbitrário ... },
  "metadata": {
    "source": "string opcional",
    "targetEndpoint": "string opcional (endpoint relativo no legado)"
  }
}
```

**Response 202 Accepted:**

```json
{
  "messageId": "01HXYZ...",
  "acceptedAt": "2026-05-25T14:30:00Z"
}
```

**Response 409 Conflict (duplicata):**

```json
{
  "error": "DUPLICATE",
  "messageId": "<ULID original>",
  "idempotencyKey": "<key>"
}
```

**Outros erros:**

| Status | Causa |
|---|---|
| 400 | `X-Idempotency-Key` ausente, payload JSON malformado |
| 413 | Payload > 1 MiB |
| 503 | Redis indisponível, produce Kafka falhou após retries |

### 3.2 Fluxo de ingress

```
1. Valida envelope (sync)
2. SET idemp:{key} {messageId} NX EX {ttl}  (Redis, Mutiny)
   ├── OK    → publica Kafka
   │           ├── OK    → 202 {messageId}
   │           └── falha → DEL idemp:{key} + 503
   └── exists → GET idemp:{key} → 409 {messageId original}
```

**TTL idempotency:** configurável via env `IDEMPOTENCY_TTL_SECONDS` (default `86400` = 24h).

**Importante:** se produce Kafka falhar APÓS gravar a chave, executar `DEL idemp:{key}` permite retry do cliente sem bloquear pela chave órfã.

### 3.3 Endpoints administrativos

| Endpoint | Descrição |
|---|---|
| `GET /admin/throttle` | Retorna `{capacity, available, refillRate}` |
| `GET /admin/circuit-breaker/status` | Estado atual do CB: CLOSED / OPEN / HALF_OPEN |
| `POST /admin/circuit-breaker/reset` | Força transição para CLOSED |

Auth simples via header `X-Admin-Token` (env var `ADMIN_TOKEN`). Não é production-grade; suficiente para ferramenta interna.

---

## 4. Envelope da mensagem Kafka

### 4.1 Topic `messages.in`

**Key:** `messageId` (ULID) — garante ordering por chave e idempotência no nível do broker.

**Value (JSON):**

```json
{
  "messageId": "01HXYZ...",
  "idempotencyKey": "<original do header>",
  "occurredAt": "2026-05-25T14:30:00Z",
  "attempt": 0,
  "traceContext": {
    "traceparent": "00-...-...-01",
    "tracestate": "..."
  },
  "metadata": {
    "source": "...",
    "targetEndpoint": "..."
  },
  "payload": { ... }
}
```

**Headers Kafka:**

- `messageId`
- `traceparent` (propagação OTel nativa)
- `content-type: application/json`

### 4.2 Topic `messages.dlq`

Mesmo envelope original, acrescido de:

```json
{
  ...envelope original...,
  "failure": {
    "reason": "LEGACY_5XX | LEGACY_TIMEOUT | LEGACY_4XX_PERMANENT | CIRCUIT_OPEN | THROTTLE_TIMEOUT | INVALID_PAYLOAD",
    "lastError": "string",
    "attempts": 3,
    "failedAt": "2026-05-25T14:35:00Z"
  }
}
```

**Config DLQ producer:**

```properties
mp.messaging.outgoing.messages-dlq.connector=smallrye-kafka
mp.messaging.outgoing.messages-dlq.topic=messages.dlq
mp.messaging.outgoing.messages-dlq.acks=all
mp.messaging.outgoing.messages-dlq.retries=10
```

Em produção: `replication.factor=3`, retenção 7 dias.

### 4.3 Idempotência ponta a ponta

- `idempotencyKey` extraído do header HTTP.
- Persistido no envelope.
- Repassado ao legado como header `Idempotency-Key`.
- Sistema NÃO deduplica internamente no consumo; o legado é a fonte de verdade de idempotência terminal.

---

## 5. Throttling + Backpressure

### 5.1 Token Bucket (Bucket4j-Redis)

**Config (com override via env):**

```properties
throttle.capacity=100
throttle.refill-tokens=100
throttle.refill-period-ms=1000
throttle.bucket-key=global-throttle
throttle.acquire-timeout-ms=30000
```

Env vars: `THROTTLE_CAPACITY`, `THROTTLE_REFILL_TOKENS`, `THROTTLE_REFILL_PERIOD_MS`, `THROTTLE_ACQUIRE_TIMEOUT_MS`.

**Bucket setup:**

```java
BucketConfiguration cfg = BucketConfiguration.builder()
    .addLimit(Bandwidth.builder()
        .capacity(capacity)
        .refillGreedy(refillTokens, Duration.ofMillis(refillPeriodMs))
        .build())
    .build();

LettuceBasedProxyManager proxyManager = LettuceBasedProxyManager
    .builderFor(redisClient)
    .build();

AsyncBucketProxy bucket = proxyManager.builder()
    .build(bucketKey.getBytes(), () -> cfg);
```

### 5.2 Aquisição (Mutiny)

```java
public Uni<Void> acquireBlocking() {
    return Uni.createFrom().completionStage(
        bucket.asAsync().consume(1, scheduler)
    ).ifNoItem().after(Duration.ofMillis(acquireTimeoutMs))
     .failWith(new ThrottleTimeoutException());
}
```

`consume(1, scheduler)` aguarda async até disponibilidade do token sem bloquear thread. Mutiny encadeia natural.

### 5.3 Backpressure no consumer Kafka

```properties
mp.messaging.incoming.messages-in.connector=smallrye-kafka
mp.messaging.incoming.messages-in.topic=messages.in
mp.messaging.incoming.messages-in.group.id=throttling-worker
mp.messaging.incoming.messages-in.failure-strategy=ignore
mp.messaging.incoming.messages-in.commit-strategy=throttled
mp.messaging.incoming.messages-in.auto.offset.reset=earliest
mp.messaging.incoming.messages-in.concurrency=1
mp.messaging.incoming.messages-in.fetch.max.wait.ms=500
```

**Por que `concurrency=1`:** com bucket global, paralelismo > 1 desperdiça tokens em-flight quando o legado está lento. Throughput é definido pelo refill rate, não pelo número de consumidores. Permite contabilidade precisa do throttle.

### 5.4 Pipeline consumer

```java
@Incoming("messages-in")
public Uni<Void> consume(Message<MessageEnvelope> msg) {
    return Uni.createFrom().item(msg)
        .chain(m -> throttle.acquireBlocking())
        .chain(m -> legacyClient.send(
            msg.getPayload().metadata().targetEndpoint(),
            msg.getPayload().idempotencyKey(),
            msg.getPayload().payload()))
        .onItem().transformToUni(resp ->
            Uni.createFrom().completionStage(msg.ack()))
        .onFailure().recoverWithUni(err -> handleFailure(msg, err));
}
```

`acquireBlocking` segura o pipeline. Reactive Messaging não solicita a próxima mensagem até `ack/nack` completar → consumer pausa naturalmente quando o bucket está vazio. Lag cresce no broker; nada é perdido.

### 5.5 Timeout de aquisição

`throttle.acquire-timeout-ms=30000` (default 30s). Se token não chega no prazo, dispara `ThrottleTimeoutException` → DLQ com `reason=THROTTLE_TIMEOUT`.

---

## 6. Resiliência

### 6.1 Camadas de falha

| Camada | Falha | Tratamento |
|---|---|---|
| Ingress · Redis | SET NX falha | 503 ao cliente |
| Ingress · Kafka produce | Timeout/falha após retries | 503 + `DEL idemp:{key}` |
| Consumer · throttle | Acquire timeout | DLQ (`THROTTLE_TIMEOUT`) |
| Consumer · parse | JSON malformado | DLQ direto (`INVALID_PAYLOAD`), sem retry |
| Consumer · legado | 5xx, timeout, IO | Retry → Circuit Breaker → DLQ |
| Consumer · legado | 4xx permanente | DLQ direto (`LEGACY_4XX_PERMANENT`) |

### 6.2 Legacy REST Client (MicroProfile Fault Tolerance)

```java
@RegisterRestClient(configKey = "legacy-api")
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
        successThreshold = 3)
    @Timeout(value = 5000)
    @Bulkhead(value = 50)
    Uni<LegacyResponse> send(@PathParam("endpoint") String endpoint,
                             @HeaderParam("Idempotency-Key") String idempKey,
                             Object payload);
}
```

### 6.3 Tunables (env)

```properties
legacy.api.url=http://legacy-system:8080
legacy.timeout-ms=5000
legacy.retry.max-attempts=3
legacy.retry.delay-ms=200
legacy.cb.failure-ratio=0.5
legacy.cb.volume-threshold=20
legacy.cb.open-duration-sec=10
legacy.cb.success-threshold=3

consumer.max-hard-retries=5
```

### 6.4 Classificação de erros do legado

```
ResponseExceptionMapper:
  4xx (exceto 408, 429) → LegacyPermanentException   (não retenta)
  408, 429, 5xx, IO, timeout → LegacyTransientException (retenta)
```

### 6.5 Comportamento do Circuit Breaker

- Janela: últimas 20 requisições.
- Abre quando ≥ 50% falham.
- Aberto por 10s → half-open.
- Half-open: 3 sucessos consecutivos → CLOSED; 1 falha → reabre.

Quando aberto: `CircuitBreakerOpenException` → mensagem vai pra DLQ com `reason=CIRCUIT_OPEN`. Bucket global continua segurando o ritmo de novas tentativas quando o CB voltar ao CLOSED.

### 6.6 DLQ handler

```java
Uni<Void> handleFailure(Message<MessageEnvelope> msg, Throwable err) {
    FailureReason reason = classify(err);
    if (reason.isPermanent() || msg.getPayload().attempt() >= MAX_HARD_RETRIES) {
        return dlqProducer.send(buildDlqEnvelope(msg, err, reason))
            .chain(() -> Uni.createFrom().completionStage(msg.ack()));
    }
    return Uni.createFrom().completionStage(msg.nack(err));
}
```

`attempt` no envelope é hard-cap de proteção (default `MAX_HARD_RETRIES=5`). Retries ordinários ficam dentro do `@Retry` síncrono do client.

---

## 7. Observabilidade (OpenTelemetry OTLP)

### 7.1 Stack

- Extension: `quarkus-opentelemetry` + `quarkus-opentelemetry-exporter-otlp`.
- Exporta traces, metrics e logs via OTLP gRPC para um único collector.
- Logs em JSON via `quarkus-logging-json`, correlacionados via MDC (`trace_id`, `span_id`).

### 7.2 Config

```properties
quarkus.application.name=kafka-throttling-system
quarkus.otel.enabled=true
quarkus.otel.exporter.otlp.endpoint=http://otel-collector:4317
quarkus.otel.exporter.otlp.protocol=grpc
quarkus.otel.traces.exporter=otlp
quarkus.otel.metrics.exporter=otlp
quarkus.otel.logs.exporter=otlp
quarkus.otel.resource.attributes=service.namespace=throttling,deployment.environment=${ENV:dev}
quarkus.otel.traces.sampler=parentbased_traceidratio
quarkus.otel.traces.sampler.arg=1.0
quarkus.log.console.json=true
```

Override env: `OTEL_EXPORTER_OTLP_ENDPOINT`.

### 7.3 Propagação de trace

Padrão **W3C `traceparent`** ponta a ponta:

1. Cliente envia `traceparent` no POST `/messages` (opcional).
2. Ingress captura span context e o serializa no envelope (`envelope.traceContext`) e em headers Kafka (`traceparent`).
3. Reactive Messaging Kafka propaga via headers automaticamente.
4. Consumer extrai e cria span filho ligado ao trace original.
5. REST client reativo propaga para o legado.

### 7.4 Hierarquia de spans

```
POST /messages                              [ingress]
├── redis.set idemp:{key}                   [ingress]
└── kafka.send messages.in                  [ingress]
        │
        └─► kafka.receive messages.in       [consumer]
            ├── throttle.acquire            [throttling]
            │   └── redis.bucket.consume    [throttling]
            ├── legacy.send                 [legacy]
            │   └── HTTP POST {endpoint}    [legacy]
            └── kafka.ack                   [consumer]
```

### 7.5 Spans customizados

```java
@WithSpan("throttle.acquire")
Uni<Void> acquireBlocking(@SpanAttribute("bucket.key") String key) { ... }

@WithSpan("dlq.send")
Uni<Void> sendToDlq(@SpanAttribute("dlq.reason") String reason, ...) { ... }
```

Atributos-chave por span: `messageId`, `idempotencyKey`, `attempt`, `throttle.wait_ms`, `legacy.endpoint`, `legacy.http.status`.

### 7.6 Métricas (Micrometer + OTel bridge)

| Métrica | Tipo | Tags |
|---|---|---|
| `messages.ingress.received` | counter | `outcome=accepted\|duplicate\|rejected` |
| `messages.ingress.idempotency.duplicate` | counter | — |
| `messages.consumed` | counter | `outcome=success\|dlq\|retry` |
| `throttle.tokens.consumed` | counter | — |
| `throttle.tokens.wait.duration` | timer (histogram) | — |
| `throttle.acquire.timeout` | counter | — |
| `throttle.bucket.available` | gauge | — |
| `legacy.request.duration` | timer | `outcome`, `status` |
| `legacy.circuit.state` | gauge | 0=closed, 1=open, 2=half-open |
| `dlq.messages.sent` | counter | `reason` |

Built-in incluem HTTP server, Kafka producer/consumer, REST client e JVM.

### 7.7 Logs estruturados

JSON via `quarkus-logging-json`. MDC injetado automaticamente:

- `trace_id`, `span_id`
- `messageId`, `idempotencyKey`
- `kafka.topic`, `kafka.partition`, `kafka.offset` (no consumer)

### 7.8 Health checks

- `GET /q/health/live` — liveness (default Quarkus)
- `GET /q/health/ready` — readiness custom:
  - Redis ping
  - Kafka `describeCluster`
- `GET /q/metrics` — backup Prometheus scrape caso OTLP esteja indisponível

---

## 8. Estrutura do projeto

```
kafka-throttling-system/
├── pom.xml
├── README.md
├── docker-compose.yml              # kafka, redis, otel-collector, wiremock (dev)
├── docs/superpowers/specs/
│   └── 2026-05-25-kafka-throttling-system-design.md
├── src/
│   ├── main/
│   │   ├── java/com/throttling/
│   │   │   ├── ingress/
│   │   │   │   ├── MessagesResource.java
│   │   │   │   ├── IngressService.java
│   │   │   │   ├── IdempotencyStore.java
│   │   │   │   └── dto/ {IngressRequest, IngressResponse, DuplicateResponse}
│   │   │   ├── processing/
│   │   │   │   ├── MessageConsumer.java
│   │   │   │   └── MessageHandler.java
│   │   │   ├── throttling/
│   │   │   │   ├── TokenBucketService.java
│   │   │   │   ├── BucketConfig.java
│   │   │   │   └── ThrottleException.java
│   │   │   ├── legacy/
│   │   │   │   ├── LegacyClient.java
│   │   │   │   ├── LegacyResponseMapper.java
│   │   │   │   └── exceptions/ {Transient, Permanent}
│   │   │   ├── dlq/
│   │   │   │   ├── DlqProducer.java
│   │   │   │   └── DlqEnvelope.java
│   │   │   ├── observability/
│   │   │   │   ├── OtelConfig.java
│   │   │   │   ├── MetricsRegistry.java
│   │   │   │   └── TraceContextPropagator.java
│   │   │   ├── admin/
│   │   │   │   └── AdminResource.java
│   │   │   └── common/
│   │   │       ├── MessageEnvelope.java
│   │   │       ├── UlidGenerator.java
│   │   │       └── Constants.java
│   │   └── resources/
│   │       ├── application.properties
│   │       └── META-INF/resources/openapi.yaml
│   └── test/
│       ├── java/com/throttling/
│       │   ├── ingress/         # unit
│       │   ├── throttling/      # unit
│       │   ├── legacy/          # unit + Wiremock
│       │   ├── integration/
│       │   │   ├── IngressIdempotencyIT.java
│       │   │   ├── ThrottleBackpressureIT.java
│       │   │   ├── CircuitBreakerDlqIT.java
│       │   │   ├── RetryTransientIT.java
│       │   │   └── EndToEndIT.java
│       │   └── e2e/
│       │       └── HappyPathE2E.java
│       └── resources/
│           ├── application-test.properties
│           └── wiremock/mappings/
└── .mvn/
```

### 8.1 Dependências Maven

```
io.quarkus:quarkus-rest
io.quarkus:quarkus-rest-jackson
io.quarkus:quarkus-rest-client-reactive-jackson
io.quarkus:quarkus-smallrye-reactive-messaging-kafka
io.quarkus:quarkus-redis-client
io.quarkus:quarkus-smallrye-fault-tolerance
io.quarkus:quarkus-smallrye-health
io.quarkus:quarkus-opentelemetry
io.quarkus:quarkus-opentelemetry-exporter-otlp
io.quarkus:quarkus-micrometer-registry-prometheus
io.quarkus:quarkus-logging-json
io.quarkus:quarkus-config-yaml

com.bucket4j:bucket4j-redis
io.lettuce:lettuce-core
com.github.f4b6a3:ulid-creator

# test
io.quarkus:quarkus-junit5
io.quarkus:quarkus-test-kafka-companion
io.rest-assured:rest-assured
org.testcontainers:kafka
org.testcontainers:junit-jupiter
com.redis:testcontainers-redis
com.github.tomakehurst:wiremock-jre8
org.assertj:assertj-core
org.awaitility:awaitility
```

---

## 9. Estratégia de testes (TDD strict)

### 9.1 Unit (Mockito, sem boot Quarkus)

- `IdempotencyStoreTest` — verifica chamadas `SET NX EX` no Redis client mockado.
- `TokenBucketServiceTest` — mock `AsyncBucketProxy`, valida emissões/falhas do Uni.
- `MessageHandlerTest` — mocka throttle + legado, verifica pipeline Mutiny via `UniAssertSubscriber`.
- `LegacyResponseMapperTest` — classificação 4xx/5xx → exceções corretas.
- `DlqProducerTest` — envelope DLQ construído corretamente.
- `MessagesResourceTest` (`@QuarkusTest`) — resource-only com serviços mockados (`@InjectMock`).

### 9.2 Integration (`@QuarkusTest` + Testcontainers)

- `IngressIdempotencyIT` — POST duas vezes com mesma key → 202 + 409, mensagem no Kafka uma única vez.
- `ThrottleBackpressureIT` — produz 50 mensagens com refill=10/s, valida consumo limitado e que o lag drena no ritmo correto.
- `CircuitBreakerDlqIT` — Wiremock simula 500s, valida CB abrindo e mensagens indo pra DLQ.
- `RetryTransientIT` — Wiremock sequência 503 → 200, valida retry com sucesso.
- `EndToEndIT` — POST → consumer → legado (Wiremock 200) → ack, verifica métricas e trace.

Recursos de teste via `QuarkusTestResourceLifecycleManager`: `KafkaTestResource`, `RedisTestResource`, `WiremockTestResource`.

### 9.3 E2E

- `HappyPathE2E` — `docker-compose up`, curl POST, `awaitility` aguarda incremento da métrica `messages.consumed{outcome=success}`.

### 9.4 CI (placeholder)

```
mvn verify                  # unit + integration
mvn test -Pe2e              # E2E opcional
mvn package -DskipTests     # jar
```

Container image via `quarkus-container-image-jib`.

---

## 10. Comandos de desenvolvimento

```bash
mvn quarkus:dev                 # dev mode, hot reload
mvn verify                      # unit + integration
mvn package -DskipTests         # jar
docker-compose up               # stack completa local
```

`docker-compose.yml` sobe: Kafka (KRaft mode), Redis 7, OpenTelemetry Collector, Wiremock simulando legado.

---

## 11. Fora de escopo (YAGNI explícito)

- Multi-tenancy / buckets por chave.
- Hot reload do throttle (config via env vars apenas).
- Replay automatizado da DLQ (manual via kafka-console-consumer/tooling externo).
- Auth/AuthZ no `/messages` (assume edge proxy/API gateway).
- Schema Registry (envelope JSON estável definido na seção 4).
- Webhook callback / status polling pro cliente.
- GraalVM native image (JVM only nesta versão; pode ser adicionada como perfil Maven depois).

---

## 12. Resumo executivo

Sistema de throttling reativo Quarkus para proteger sistema legado:

1. **Ingress idempotente** via Redis `SET NX` antes de publicar Kafka — duplicatas retornam 409.
2. **Token bucket global** via Bucket4j-Redis controla ritmo de consumo.
3. **Backpressure natural** via SmallRye Reactive Messaging com `concurrency=1` e aquisição assíncrona Mutiny.
4. **Resiliência em camadas**: classificação trans/perm + Retry + Circuit Breaker + DLQ.
5. **Observabilidade unificada OTLP**: traces propagados W3C ponta a ponta (HTTP → Kafka → legado), metrics Micrometer custom, logs JSON correlacionados.
6. **Testes TDD strict** com Testcontainers (Kafka, Redis) e Wiremock (legado).
