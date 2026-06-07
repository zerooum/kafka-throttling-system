# Verificação em tabela e retentativa após erro da API externa

**Data:** 2026-06-06
**Status:** Aprovado para planejamento

## Problema

A chamada à API externa (legado) tem timeout. Um timeout **não** significa que a
requisição não foi processada — apenas que o legado não respondeu a tempo. Retentar
às cegas pode causar duplo-processamento.

O legado grava um registro numa tabela quando conclui o processamento, indexado pela
`idempotencyKey` enviada no header `Idempotency-Key`. Após um erro retentável, a
aplicação deve **aguardar** e **consultar essa tabela** antes de decidir retentar:

- Registro presente → o legado processou (apesar do timeout) → **ack**, sem retentar.
- Registro ausente → realmente não processou → **retenta** a chamada.

Esse ciclo repete até **3 chamadas** à API. Se na 3ª ainda não houver registro nem
sucesso, a mensagem vai para a **DLQ**.

## Regras

- **Dispara o ciclo:** timeout e erros transientes 5xx (`LegacyTransientException`,
  `TimeoutException`).
- **Não dispara (falha imediata → DLQ):** 4xx permanente (`LegacyPermanentException`),
  circuito aberto (`CircuitBreakerOpenException`), throttle timeout
  (`ThrottleTimeoutException`). Sem espera nem consulta.
- **Máximo de 3 chamadas** à API externa por mensagem.
- Mesmo na 3ª falha retentável, faz espera + consulta antes de desistir. Só vai para a
  DLQ se a tabela seguir vazia. Evita DLQ falso quando o legado processou tarde.
- App **apenas lê** a tabela. O legado escreve. (No mock/teste, `MockLegacyResource`
  simula a escrita.)
- Consulta pela `idempotencyKey`.

## Arquitetura

Abordagem: orquestrador + repositório dedicados, mantendo o estilo do projeto
(serviços pequenos, um propósito cada, comunicação via `Uni`).

### Fluxo no consumidor

```
MessageConsumer.consume
  └─ MessageHandler.handle
       └─ RetryOrchestrator.execute(env)   ← novo
            success → ack
            failure → MessageHandler.sendToDlq (classify + DlqProducer, inalterado)
```

### Lógica do RetryOrchestrator

`execute(MessageEnvelope): Uni<Void>` — máximo 3 chamadas (`attempt` 1..3):

```
attempt(n):
  throttle.acquireBlocking()            // re-adquire token por tentativa
  → legacy.send(endpoint, idempotencyKey, payload)
      sucesso            → Uni<Void> (ack)
      falha:
        classify(err):
          não-retentável  → propaga a falha imediatamente (→ DLQ), sem espera/consulta
          retentável (timeout, 5xx):
            delay backoff(n)            // não-bloqueante (Mutiny)
            verify.exists(idempotencyKey):
              true   → Uni<Void> (ack — legado processou apesar do timeout)
              false  → n < maxAttempts ? attempt(n+1) : propaga a falha (→ DLQ)
```

O orquestrador retorna um `Uni<Void>` que **completa** nos caminhos de ack e **falha**
(com o último erro) quando a mensagem deve ir para a DLQ. Assim o
`onFailure().recoverWithUni(sendToDlq)` do `MessageHandler` permanece inalterado.

## Componentes

### Novos

- **`VerificationRepository`** (`@ApplicationScoped`, Panache reativo)
  - `exists(String idempotencyKey): Uni<Boolean>`
  - Consulta a tabela `processing_record`.

- **`ProcessingRecord`** (entidade Panache reativa)
  - Tabela `processing_record`: `idempotency_key` (PK, varchar), `processed_at`
    (timestamp).

- **`BackoffPolicy`** (`@ApplicationScoped`, puro)
  - `delayForAttempt(int n): Duration`
  - Config: `base-delay=1s`, `backoff-multiplier=2` → 1s, 2s, 4s.

- **`RetryOrchestrator`** (`@ApplicationScoped`)
  - `execute(MessageEnvelope): Uni<Void>`
  - Depende de: `TokenBucketService`, `LegacyClient`, `VerificationRepository`,
    `BackoffPolicy`, `MetricsRegistry`.

### Alterados

- **`LegacyClient`** — remove `@Retry`. Mantém `@Timeout`, `@CircuitBreaker`,
  `@CircuitBreakerName`, `@Bulkhead`.
- **`MessageHandler`** — troca `.chain(() -> legacy.send(...))` por
  `.chain(() -> orchestrator.execute(env))`. Remove a aquisição de throttle daqui
  (passa para o orquestrador, por tentativa). `sendToDlq` e `classify` permanecem.
- **`MockLegacyResource`** — ao processar com sucesso, insere linha em
  `processing_record` (`idempotency_key` = header `Idempotency-Key`). Permite
  exercitar o caminho "achou na tabela" no mock e na tela de testes.
- **`MetricsRegistry`** — novos contadores `verifyChecked{result=found|empty}` e
  `apiRetried`.

## Infra

- **pom:** `quarkus-hibernate-reactive-panache`, `quarkus-reactive-pg-client`.
- **docker-compose:** serviço `postgres`.
- **Schema:** gerado pela entidade Panache via `quarkus.hibernate-orm.database.generation`:
  `drop-and-create` em `%dev`/`%test`, `none` em prod (a tabela pertence ao legado).
  Sem Flyway — evita um datasource JDBC bloqueante só para migrations.

## Config

```properties
throttle.verify.max-attempts=3
throttle.verify.base-delay=1s
throttle.verify.backoff-multiplier=2

# datasource reativo
quarkus.datasource.db-kind=postgresql
quarkus.datasource.reactive.url=postgresql://localhost:5432/throttling
```

## Tratamento de erro / DLQ

- Esgotadas as 3 chamadas com tabela vazia → propaga o último erro → `sendToDlq`.
- `FailureReason` = classificação do último erro (`LEGACY_TIMEOUT` ou `LEGACY_5XX`).
- `attempt` no envelope DLQ permanece o `env.attempt()` atual (contador de entrega do
  Kafka). A contagem interna de chamadas à API não é modelada no `DlqEnvelope` (YAGNI).
- Falhas não-retentáveis → DLQ imediato, comportamento e reason atuais preservados.

## Testes (TDD)

- **`BackoffPolicyTest`** — unit: delays corretos por tentativa, respeita config.
- **`RetryOrchestratorTest`** — unit, com mocks de `LegacyClient` e
  `VerificationRepository`:
  - sucesso na 1ª chamada → ack, sem espera/consulta;
  - timeout → consulta acha registro → ack;
  - timeout × 3 + tabela vazia → falha propagada (→ DLQ), 3 chamadas;
  - 5xx → consulta acha registro → ack;
  - 4xx permanente → falha imediata, sem espera/consulta;
  - throttle re-adquirido por tentativa.
- **`VerificationRepositoryTest`** — integração com `PostgresTestResource`
  (testcontainer, estilo `RedisTestResource`).
- **`MessageHandlerTest`** — atualizado para delegar ao orquestrador.
- **Verificação end-to-end manual** via tela de testes (dev): `MockLegacyResource`
  grava a linha e, com flag `simulateTimeout`, atrasa além do timeout do client →
  exercita o caminho timeout→achou→ack. IT automatizado multi-container fica como
  trabalho futuro (YAGNI agora; cobertura real vem dos testes unitários + de repositório).

## Fora de escopo (YAGNI)

- App escrever na tabela (registro 'pending'). Só leitura.
- Backoff configurável por tipo de erro. Uma política só.
- Retentativa após DLQ.
