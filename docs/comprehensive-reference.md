# Kafka Ledger Learning Platform — Comprehensive Documentation

> **Java 21 · Spring Boot 3.2 · Kafka Streams 3.6 · Confluent 7.6 · Avro · Debezium · RocksDB**

This document is the single, self-contained reference for every component,
pattern, and design decision in the project. It is organised so you can read
it end-to-end or jump to any section independently.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Infrastructure (Docker Compose)](#2-infrastructure-docker-compose)
3. [Avro Schemas & Schema Registry](#3-avro-schemas--schema-registry)
4. [Topology 1 — Ledger Balances](#4-topology-1--ledger-balances)
5. [Topology 2 — Fraud Detection (Processor API)](#5-topology-2--fraud-detection-processor-api)
6. [Topology 3 — Payment Orchestration State Machine](#6-topology-3--payment-orchestration-state-machine)
7. [Topology 4 — Windowed Join (Payment Matching)](#7-topology-4--windowed-join-payment-matching)
8. [Topology 5 — Revenue Aggregation (Tumbling Windows)](#8-topology-5--revenue-aggregation-tumbling-windows)
9. [Topology 6 — CDC Merge (Debezium + Kafka Streams)](#9-topology-6--cdc-merge-debezium--kafka-streams)
10. [Interactive Queries & REST API](#10-interactive-queries--rest-api)
11. [Multi-Tenant SaaS Architecture](#11-multi-tenant-saas-architecture)
12. [Exactly-Once Semantics (EOS)](#12-exactly-once-semantics-eos)
13. [RocksDB Tuning](#13-rocksdb-tuning)
14. [Error Handling & Dead Letter Queues](#14-error-handling--dead-letter-queues)
15. [Serialization Strategy](#15-serialization-strategy)
16. [Observability & Metrics](#16-observability--metrics)
17. [Configuration Reference](#17-configuration-reference)
18. [Kubernetes Deployment](#18-kubernetes-deployment)
19. [Terraform (Infrastructure as Code)](#19-terraform-infrastructure-as-code)
20. [Testing Strategy](#20-testing-strategy)
21. [Project File Map](#21-project-file-map)
22. [How to Run](#22-how-to-run)

---

## 1. Architecture Overview

The application is a **multi-topology Kafka Streams** application running inside
a single Spring Boot process. All topologies share one `StreamsBuilder`
(one Kafka Streams `application-id`), which means they share consumer-group
membership, state management, and exactly-once guarantees.

```
                           ┌─────────────────────────────────────────────┐
                           │          Spring Boot Application            │
                           │                                             │
  REST API                 │  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
  ─────────────────────▶   │  │  Ledger   │  │ Payment  │  │ Revenue  │ │
  POST /api/transactions   │  │ Topology  │  │ Topology │  │ Topology │ │
  POST /api/payments       │  └─────┬─────┘  └────┬─────┘  └────┬─────┘ │
  GET  /api/ledger/...     │        │              │             │       │
  GET  /api/payments/...   │  ┌─────┴─────┐  ┌────┴─────┐  ┌───┴──────┐│
  GET  /api/revenue/...    │  │  Fraud     │  │ Windowed │  │   CDC    ││
  GET  /api/fraud/...      │  │ Processor  │  │  Join    │  │  Merge   ││
  GET  /api/sync-runs/...  │  └─────┬─────┘  └────┬─────┘  └───┬──────┘│
                           │        │              │             │       │
                           │  ══════╪══════════════╪═════════════╪═══   │
                           │     RocksDB State Stores (10 stores)       │
                           └────────┼──────────────┼─────────────┼──────┘
                                    │              │             │
                           ─────────┴──────────────┴─────────────┴──────
                                         Apache Kafka
                              (13 topics · Schema Registry · Connect)
```

### Data Flow Summary

| Flow | Source | Topology | Output Topic | State Store |
|------|--------|----------|--------------|-------------|
| Ledger | `tx-events` | `LedgerTopologyFactory` + `TransactionGuardProcessor` | `ledger-balances` / `ledger-dlq` | `ledger-balance-store`, `tx-dedup-store`, `account-currency-store` |
| Fraud | `tx-events` (accepted only) | `FraudDetectionProcessor` | `fraud-alerts` | `fraud-activity-store`, `fraud-expiry-index-store` |
| Payment | `payment-commands` | `PaymentTopologyFactory` | `payment-events` / `payment-dlq` | `payment-state-store` |
| Match | `payment-events` | `PaymentWindowedJoinTopologyFactory` | `payment-matched` | (internal join stores) |
| Revenue | `payment-events` | `RevenueAggregationTopologyFactory` | `tenant-revenue-hourly` | `revenue-hourly-store` |
| CDC | `sync.public.*` | `SyncRunCdcTopologyFactory` | `sync-runs-enriched` | `sync-runs-merged-store` |

---

## 2. Infrastructure (Docker Compose)

`docker-compose.yml` provisions the full Confluent Platform stack:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| **zookeeper** | `cp-zookeeper:7.6.1` | 2181 | Cluster coordination |
| **kafka** | `cp-kafka:7.6.1` | 9092 (external) / 29092 (internal) | Message broker |
| **schema-registry** | `cp-schema-registry:7.6.1` | 8081 | Avro schema storage & compatibility |
| **postgres** | `postgres:16` | 5433→5432 | CDC source database |
| **connect** | `cp-kafka-connect:7.6.1` | 8083 | Debezium PostgreSQL CDC connector |
| **kafka-ui** | `provectuslabs/kafka-ui` | 8085 | Web dashboard |

### Key Docker Details

- **Kafka listeners**: `INTERNAL://kafka:29092` for inter-container traffic, `EXTERNAL://localhost:9092` for host access.
- **Postgres** starts with `wal_level=logical`, `max_wal_senders=10`, `max_replication_slots=10` — required for Debezium CDC.
- **Connect** auto-installs `debezium/debezium-connector-postgresql:2.5.4` on startup.
- **Init SQL** (`connect/postgres-init-sync.sql`) creates `sync_runs` and `sync_run_payloads` tables with a `generate_nanoid()` function for primary keys.

```bash
# Start everything
docker compose up -d

# Check health
docker compose ps

# View Kafka UI
open http://localhost:8085
```

---

## 3. Avro Schemas & Schema Registry

All Avro schemas live in `src/main/avro/` and are compiled to Java by the
`avro-maven-plugin` during `generate-sources`.

| Schema | File | Key Fields |
|--------|------|------------|
| `TransactionEvent` | `TransactionEvent.avsc` | `eventId`, `tenantId`, `accountId`, `amount`, `direction` (DEBIT/CREDIT), `currency`, `eventTime` |
| `LedgerBalance` | `LedgerBalance.avsc` | `tenantId`, `accountId`, `balance`, `currency`, `lastEventTime` |
| `FraudAlert` | `FraudAlert.avsc` | `tenantId`, `accountId`, `eventId`, `reason`, `amount`, `eventTime` |
| `PaymentEvent` | `PaymentEvent.avsc` | `paymentId`, `tenantId`, `accountId`, `amount`, `currency`, `commandType` (INITIATE/AUTHORIZE/CAPTURE/SETTLE/REVERSE), `correlationId`, `eventTime` |
| `TenantRevenue` | `TenantRevenue.avsc` | `tenantId`, `windowStart`, `windowEnd`, `totalAmount`, `transactionCount`, `currency` |

### Money Representation

Every monetary field (`amount`, `balance`, `totalAmount`) is an Avro **`decimal(19,4)`** logical
type, generated as `java.math.BigDecimal` (`enableDecimalLogicalType` in the Avro plugin). Binary
floating point cannot represent most decimal fractions (`0.1 + 0.2 ≠ 0.3` in `double`), which is
unacceptable in a ledger.

`model/Money` is the single place amounts are normalised: exactly 4 fractional digits, at most 19
significant digits, never rounded (excess precision is a `400`, not a silent rounding), and
ISO-4217 currency codes validated via `java.util.Currency`.

### Schema Registry Integration

- **Auto-registration** is enabled (`auto.register.schemas: true`).
- Avro serdes are created via `SpecificAvroSerde<T>` in `KafkaStreamsConfig`.
- The Schema Registry URL is configured once in `application.yml` and propagated to all serde beans.
- **Subject naming**: default `TopicNameStrategy` — schema registered as `{topic}-value`.

### Schema Evolution Rules

Confluent Schema Registry enforces **backward compatibility** by default:
- You can **add** fields with defaults.
- You can **remove** optional fields.
- You **cannot** rename fields or change types.

---

## 4. Topology 1 — Ledger Balances

**File:** `LedgerTopologyFactory.java`

Consumes `TransactionEvent` messages from `tx-events`, re-keys and explicitly repartitions them
by `tenantId|accountId`, passes them through an ingress guard, and aggregates a running balance
in a KTable materialised to the `ledger-balance-store`.

### Ingress Guard (`TransactionGuardProcessor`)

Exactly-once only covers Kafka-internal read-process-write cycles. A client that retries an HTTP
POST produces a *second, distinct* record, which EOS will faithfully apply twice. The guard runs
before anything touches a balance:

- **Idempotency** — each `eventId`, scoped to its `tenantId|accountId` key (tenants choose their
  own IDs), is remembered in a window store (`tx-dedup-store`) for `app.ledger.dedup-window`
  (default 7 days, matching `tx-events` retention). Replays are dropped and counted.
- **Single-currency accounts** — the first accepted event pins the account's currency in
  `account-currency-store`; an event in another currency is routed to `ledger-dlq` with a
  `dlq.reason` header instead of being summed into the balance.

The explicit `repartition()` matters: the guard must see every event (and every replay) for an
account on the same task, regardless of how upstream producers keyed `tx-events`.

### How Balance Is Calculated

```java
BigDecimal signedAmount = event.getDirection() == TxDirection.DEBIT
        ? event.getAmount().abs().negate()   // Debits subtract
        : event.getAmount().abs();           // Credits add

newBalance = Money.normalize(currentBalance.add(signedAmount));
```

The aggregate starts at `Money.ZERO` (`0.0000`). Every accepted event writes the new
`LedgerBalance` snapshot to both the state store and the `ledger-balances` topic (compacted).

### Topology Diagram

```
tx-events (Avro)
    │
    ▼  selectKey → tenantId|accountId
    ▼  repartition ("tx-by-account")
    │
    ▼  processValues(TransactionGuardProcessor)   ── tx-dedup-store, account-currency-store
    │        │
    │        ├── duplicate          → dropped (ledger.transactions.duplicates++)
    │        ├── currency mismatch  → ledger-dlq (TransactionEvent + dlq.reason header)
    │        └── accepted
    │               │
    │               ├── groupByKey → aggregate(balance + signed amount)
    │               │         materialised as → ledger-balance-store
    │               │         toStream → ledger-balances (Avro, compacted)
    │               │
    │               └── FraudDetectionProcessor → fraud-alerts
```

### State Stores

| Store | Type | Key | Value |
|-------|------|-----|-------|
| `ledger-balance-store` | Persistent KeyValue | `tenantId\|accountId` | `LedgerBalance` (Avro) |
| `tx-dedup-store` | Persistent Window | `tenantId\|accountId\|eventId` | event timestamp |
| `account-currency-store` | Persistent KeyValue | `tenantId\|accountId` | ISO-4217 code |

---

## 5. Topology 2 — Fraud Detection (Processor API)

**File:** `FraudDetectionProcessor.java`

### Why the Processor API?

The Kafka Streams DSL (`map`, `filter`, `aggregate`) covers most use cases,
but when you need **direct state-store access**, **scheduled processing
(Punctuators)**, or **conditional forwarding**, the native Processor API is
required.

This project uses the **modern Processor API** (Kafka 3.3+):
- `FixedKeyProcessor<KIn, VIn, VOut>` — preserves the record key
- `FixedKeyProcessorContext` — typed context with `forward()` and `schedule()`
- Invoked via `KStream.processValues()` in the DSL

### Fraud Rules

| Rule | Trigger | Alert Reason |
|------|---------|--------------|
| Single-amount threshold | `abs(amount) >= 10,000` | `SINGLE_TX_THRESHOLD_EXCEEDED` |
| Burst activity | `≥ 5 events in 60 seconds` | `BURST_ACTIVITY_DETECTED` |

Both thresholds are configurable via `LedgerStreamProperties` (`app.ledger.*`
in `application.yml`). The fraud processor only sees events the ingress guard accepted, so
replayed events cannot inflate the burst count.

### Punctuator (Scheduled Processing)

A **stream-time** `Punctuator` runs every 30 seconds of event time and evicts accounts idle for
more than `2 × burstWindowMs`, preventing unbounded state growth.

```java
context.schedule(
    Duration.ofSeconds(30),
    PunctuationType.STREAM_TIME,
    this::expireIdleAccounts
);
```

Two design choices:

- **Stream time, not wall-clock time.** `lastEventTime` is event time, so the eviction clock must
  be event time too. Comparing it with the wall clock evicts everything immediately when
  replaying history (so bursts in replayed data are never detected) and never evicts while
  processing a backlog.
- **Expiry index instead of a full scan.** `fraud-expiry-index-store` is keyed by
  `zeroPaddedLastEventTime|accountKey`. Zero-padding makes lexicographic key order equal numeric
  time order, so each punctuation range-scans only the expired prefix rather than every account.
  Each update deletes the account's previous index entry, and `lastEventTime` only moves forward,
  so out-of-order events cannot shorten an account's lifetime.

### How It Integrates with the DSL

```java
accepted.<FraudAlert>processValues(
    () -> new FraudDetectionProcessor(properties),
    Named.as("fraud-detector"),
    Topics.FRAUD_ACTIVITY_STORE, Topics.FRAUD_EXPIRY_INDEX_STORE
)
.filter((key, alert) -> alert != null)
.peek((key, alert) -> metrics.fraudAlert(alert.getReason()))
.to(Topics.FRAUD_ALERTS, Produced.with(stringSerde, fraudAlertSerde));
```

`processValues()` preserves the key while the processor decides whether to
forward a `FraudAlert` or nothing. Only non-null alerts pass the downstream
filter and reach the `fraud-alerts` topic.

### State Store

| Store | Type | Key | Value |
|-------|------|-----|-------|
| `fraud-activity-store` | Persistent KeyValue | `tenantId\|accountId` | `AccountActivity` (JSON) |
| `fraud-expiry-index-store` | Persistent KeyValue | `paddedLastEventTime\|tenantId\|accountId` | account key |

---

## 6. Topology 3 — Payment Orchestration State Machine

**Files:** `PaymentTopologyFactory.java`, `PaymentStateMachine.java`

### Payment Lifecycle

```
UNKNOWN ──INITIATE──▶ INITIATED
                          │
                     AUTHORIZE
                          ▼
                      AUTHORIZED ──REVERSE──▶ REVERSED
                          │
                       CAPTURE
                          ▼
                      CAPTURED ──REVERSE──▶ REVERSED
                          │
                       SETTLE
                          ▼
                      SETTLED (terminal)
```

### How It Works

1. **REST API** (`POST /api/payments`) validates a `PaymentRequest` (an unknown `commandType` is a `400`), converts it to an Avro `PaymentEvent`, produces to `payment-commands`, and answers `202` once the broker acknowledges the write.
2. The topology re-keys by `tenantId|paymentId` and uses `groupByKey().aggregate()` with `PaymentStateMachine.transition()` as the aggregation function.
3. `PaymentStateMachine` is a **pure function** — given `(currentState, command) → newState`. It uses Java 21 `switch` expressions with pattern matching.
4. **Valid transitions** yield a new state with `failReason = null` → routed to `payment-events`.
5. **Invalid transitions** (e.g., SETTLE from INITIATED) keep the status unchanged but set `failReason` → routed to `payment-dlq`.

### Topology Diagram

```
payment-commands (Avro PaymentEvent)
    │
    ▼  selectKey → tenantId|paymentId
    │
    ▼  groupByKey → aggregate(PaymentStateMachine::transition)
    │         materialised as → payment-state-store (RocksDB)
    │
    ├── filter(failReason == null) → payment-events (JSON, compacted)
    └── filter(failReason != null) → payment-dlq (JSON, 30-day retention)
```

### PaymentState Record

```java
public record PaymentState(
    String paymentId, String tenantId, String accountId,
    BigDecimal amount, String currency, String status, String failReason,
    long initiatedAt, long authorizedAt, long capturedAt,
    long settledAt, long reversedAt, long lastEventTime
)
```

---

## 7. Topology 4 — Windowed Join (Payment Matching)

**File:** `PaymentWindowedJoinTopologyFactory.java`

Demonstrates **KStream–KStream windowed joins** — one of the most important
patterns for financial platforms. Correlates INITIATED payments with their
CAPTURED confirmations within a bounded time window.

### How It Works

1. Reads from `payment-events` (produced by Topology 3).
2. Splits into two sub-streams: INITIATED and CAPTURED.
3. Performs a `KStream.join()` with `JoinWindows.ofTimeDifferenceWithNoGrace(10 minutes)`.
4. Matched pairs are emitted to `payment-matched` with the `processingTimeMs`.

```
payment-events
    │
    ├── filter(INITIATED) ──┐
    │                       ├── KStream-KStream join (10 min window)
    └── filter(CAPTURED) ───┘           │
                                        ▼
                                 payment-matched
```

### Join Semantics

- Records with the **same key** (`tenantId|paymentId`) within 10 minutes are matched.
- **Inner join**: both sides must be present.
- Kafka Streams internally creates temporary state stores for both sides of the window.

---

## 8. Topology 5 — Revenue Aggregation (Tumbling Windows)

**File:** `RevenueAggregationTopologyFactory.java`

Computes **hourly revenue per tenant and currency** using tumbling time windows on
settled payments. Amounts in different currencies are never summed.

### How It Works

1. Reads from `payment-events`, filters for `status == SETTLED`.
2. Re-keys by `tenantId|currency`.
3. Applies `TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1))`.
4. Aggregates `totalAmount` and `transactionCount` within each window.
5. Materialises into a **WindowStore** (`revenue-hourly-store`).
6. Embeds window boundaries from the `Windowed<String>` key into the output `TenantRevenue`.

```
payment-events
    │
    ▼  filter(SETTLED) → selectKey(tenantId|currency)
    │
    ▼  windowedBy(1-hour tumbling) → aggregate(sum, count)
    │     materialised as → revenue-hourly-store (WindowStore)
    │
    ▼  tenant-revenue-hourly (Avro)
```

### Windowed Interactive Query

`LedgerQueryService.findHourlyRevenue(tenantId, currency, from, to)` queries the WindowStore.
The aggregator cannot see its window, so the `windowStart`/`windowEnd` fields of the *stored*
value are placeholders; query results take the real bounds from the window-store iterator key:

```java
try (WindowStoreIterator<TenantRevenue> iter = store.fetch(revenueKey(tenantId, currency), from, to)) {
    while (iter.hasNext()) {
        KeyValue<Long, TenantRevenue> entry = iter.next();   // entry.key = window start
        results.add(RevenueAggregationTopologyFactory.toWindowEntry(entry.key, entry.value));
    }
}
```

---

## 9. Topology 6 — CDC Merge (Debezium + Kafka Streams)

**Files:** `SyncRunCdcTopologyFactory.java`, `DebeziumCdcParser.java`

Captures PostgreSQL table changes via Debezium and merges the parent
(`sync_runs`) and child (`sync_run_payloads`) tables into a single
enriched Kafka topic.

```
PostgreSQL (sync DB)
    │  WAL changes (logical replication)
    ▼
Debezium Connector (Kafka Connect)
    │
    ├── sync.public.sync_runs ──────────┐
    │                                    ├── Kafka Streams KTable leftJoin
    └── sync.public.sync_run_payloads ──┘           │
                                                     ▼
                                            sync-runs-enriched (compacted)
```

### How the Merge Works

1. **sync_runs CDC** → parsed by `DebeziumCdcParser` → re-keyed by `sync_run_id` → KTable.
2. **sync_run_payloads CDC** → parsed → re-keyed by `sync_run_id` → grouped → aggregated into `SyncRunPayloadAggregate`.
3. **Left join** by `sync_run_id` → `SyncRunMergedRecord` containing parent + child payloads.

### Debezium Envelope

```json
{ "op": "c", "before": null, "after": { "sync_run_id": "abc123", "status": "STARTED" } }
```

---

## 10. Interactive Queries & REST API

**Files:** `LedgerQueryService.java`, `InteractiveQueryRouter.java`

Interactive queries read RocksDB state stores directly, without hitting Kafka brokers.

### Routing Across Instances

Each instance only holds the store partitions assigned to it; with N replicas a local-only
lookup would miss for roughly (N-1)/N of keys. Every instance therefore advertises
`application.server` (`${APP_ADVERTISED_HOST}:${server.port}` — the pod's stable StatefulSet DNS
name in k8s), and `InteractiveQueryRouter`:

1. asks Kafka Streams `queryMetadataForKey(store, key)` for the active host;
2. answers from the local store if that is this instance;
3. otherwise forwards the HTTP call to the owner, passing the caller's `Authorization` header and
   an `X-Ledger-IQ-Forwarded` marker. A forwarded request is always answered locally, so a stale
   view of the assignment during a rebalance cannot cause forwarding loops.

While Kafka Streams is starting, rebalancing or restoring, queries return **`503` with
`Retry-After`** rather than a `500`. With `application.server` unset (single instance), every
query is local.

### Endpoints

| Method | Path | Store | Response |
|--------|------|-------|----------|
| `POST` | `/api/transactions` | — | `202` after broker ack; `400` invalid; `503` publish failed |
| `POST` | `/api/payments` | — | `202` after broker ack; `400` invalid; `503` publish failed |
| `GET` | `/api/ledger/{tenantId}/{accountId}` | `ledger-balance-store` | `BalanceView` |
| `GET` | `/api/fraud/{tenantId}/{accountId}` | `fraud-activity-store` | `FraudStateView` |
| `GET` | `/api/payments/{tenantId}/{paymentId}` | `payment-state-store` | `PaymentState` |
| `GET` | `/api/revenue/{tenantId}?currency=&from=&to=` | `revenue-hourly-store` | `List<RevenueWindowEntry>` |
| `GET` | `/api/sync-runs/{syncRunId}` | `sync-runs-merged-store` | `SyncRunMergedRecord` |

---

## 11. Multi-Tenant SaaS Architecture

### Key Convention

All composite keys follow `tenantId|subKey` — e.g. `tenant-a|acct-1` or `tenant-a|pay-1` — and
are partitioned by the default partitioner on the **full** key. Every ID is validated against
`[A-Za-z0-9._-]{1,64}` (`model/Identifiers`), so no component can contain the `|` delimiter and
forge another tenant's key.

**Why not partition by tenant alone?** It was considered and rejected: one large tenant would pin
a single partition — and a single stream thread — while nothing downstream needs cross-account
ordering within a tenant.

### Tenant Isolation at the API (`security/`)

The service is a stateless OAuth2 resource server (`SecurityConfig`). Every `/api` call needs a
bearer JWT from the issuer in `spring.security.oauth2.resourceserver.jwt.issuer-uri`, and
`TenantAccess` requires the token's `tenant_id` claim to match the tenant named in the path or
body — on writes and reads alike, including queries forwarded between pods. A mismatch is a `403`;
the tenant in a request is never trusted on its own.

Security is on unless `app.security.enabled=false`. The local `application.yml` sets it to `false`
(with a startup warning) so the curl examples work without an identity provider; the k8s profile
turns it on. Sync runs come from a CDC schema without a tenant column, so they require
authentication but no tenant match.

### TenantRoutingInterceptor (`config/TenantRoutingInterceptor.java`)

Kafka `ProducerInterceptor` that adds an `X-Tenant-Id` header to every outgoing record for downstream filtering without deserialisation.

---

## 12. Exactly-Once Semantics (EOS)

```yaml
processing.guarantee: exactly_once_v2
```

- **Atomic read-process-write**: consume, update state, produce — all in one transaction.
- **No duplicates on rebalance**: new owner replays exactly once.
- **Fenced zombies**: stale producers are blocked from writing.
- Critical for ledger balances, payment orchestration, and revenue aggregation.

**What EOS does not cover:** duplicates that arrive from *outside* Kafka — a client retrying a
POST produces a new record. Business-level idempotency is handled by the ledger's `eventId`
de-duplication (§4), which is why the ingest API can safely tell clients to retry on `503`.

---

## 13. RocksDB Tuning

**File:** `config/RocksDbConfig.java`

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Block cache | 64 MB (`LRUCache`) | Keeps hot data in memory |
| Bloom filter | 10 bits/key | Reduces false-positive disk reads |
| Write buffer | 16 MB × 3 | Absorbs write bursts |
| Compaction | Universal | Optimised for write-heavy workloads |
| Compression | LZ4 | Fast, low CPU overhead |
| Background jobs | 4 | Parallel compaction |

---

## 14. Error Handling & Dead Letter Queues

The guiding rule for a ledger: **never drop a record silently.** A record is either applied,
durably dead-lettered, or processing stops.

| Layer | Handler | Behaviour |
|-------|---------|-----------|
| Deserialization | `StreamsDeserializationErrorHandler` | Writes original bytes to `dlq-deserialization-errors`, then skips. If the DLQ write fails → `FAIL` |
| Production | `StreamsProductionExceptionHandler` | Always `FAIL` — `CONTINUE` would commit the input offset and lose a balance update |
| Uncaught (any processor) | `StreamsUncaughtExceptionHandler` in `KafkaStreamsConfig` | `SHUTDOWN_CLIENT` — failures are usually deterministic per record, so a replacement thread would loop |
| Business: invalid payment transition | `PaymentTopologyFactory` branch | `payment-dlq` |
| Business: currency mismatch | `TransactionGuardProcessor` branch | `ledger-dlq` with `dlq.reason` header |

`DeadLetterQueueHandler.enrichHeaders()` copies the original headers and adds
`dlq.original.topic|partition|offset`, `dlq.error.class`, `dlq.error.message` and a truncated
`dlq.error.stacktrace`.

**Making failures loud:** the `kafkaStreams` health indicator reports `DOWN` once the client is in
`ERROR`/`NOT_RUNNING` and is part of the liveness and readiness groups, so Kubernetes restarts the
pod instead of letting it serve HTTP while no longer processing. A deterministic poison record in
a business topology will therefore crash-loop the pod — deliberately: for a ledger, stopping is
safer than losing money.

**DLQ delivery is at-least-once.** The DLQ producer is separate from the transactional Streams
producer, so a crash between the DLQ write and the offset commit can dead-letter the same record
twice; DLQ consumers should de-duplicate on the `dlq.original.*` coordinates.

---

## 15. Serialization Strategy

| Approach | Used For | Why |
|----------|----------|-----|
| **Avro + Schema Registry** | Financial events (`TransactionEvent`, `PaymentEvent`, `LedgerBalance`, `FraudAlert`, `TenantRevenue`) | Schema evolution, type safety |
| **JSON (`JsonSerde`)** | State stores (`PaymentState`, `AccountActivity`), CDC records | Flexibility, no registry dependency |

---

## 16. Observability & Metrics

- **Prometheus** at `/actuator/prometheus` via `micrometer-registry-prometheus`.
- **Business counters** (`config/LedgerMetrics`, incremented from the topologies and asserted in
  the topology tests):

  | Metric | Tags | Incremented when |
  |--------|------|------------------|
  | `ledger_transactions_processed_total` | — | A transaction is applied to a balance |
  | `ledger_transactions_duplicates_total` | — | A replayed `eventId` is dropped |
  | `payments_state_transitions_total` | `status` | A valid payment transition is emitted |
  | `fraud_alerts_emitted_total` | `reason` | A fraud alert is emitted |
  | `dlq_records_total` | `topic` | A record is written to any DLQ |

  Under EOS, an aborted-and-retried transaction can count a record twice: these are operational
  signals, not reconciliation figures.
- **Structured logging**: JSON for `k8s`/`prod` profiles, human-readable for `default`.
- **Health checks**: `/actuator/health/liveness` and `/readiness`, both including the
  `kafkaStreams` indicator (see §14).

---

## 17. Configuration Reference

See `src/main/resources/application.yml` for all settings. Key properties:

- `application-id: kafka-ledger-learning-v2` — bumped because the decimal schemas and repartitioned
  ledger topology are incompatible with v1 state
- `processing.guarantee: exactly_once_v2` — EOS
- `application.server: ${APP_ADVERTISED_HOST:localhost}:${server.port}` — interactive-query routing
- `rocksdb.config.setter: RocksDbConfig` — custom tuning
- `default.timestamp.extractor: TransactionEventTimestampExtractor` — event-time
- `default.deserialization.exception.handler` / `default.production.exception.handler` — see §14
- `interceptor.classes: TenantRoutingInterceptor` — tenant header enrichment
- `producer.properties.max.block.ms: 5000`, `delivery.timeout.ms: 15000` — bound how long an API
  call waits for the broker ack
- `app.ledger.dedup-window: 7d` — `eventId` de-duplication horizon
- `app.security.enabled` — JWT auth + tenant isolation (`false` locally, `true` in k8s)
- `management.endpoint.health.group.{liveness,readiness}.include` — adds `kafkaStreams`

---

## 18. Kubernetes Deployment

`k8s/` uses **Kustomize** with base + overlay pattern:

- **Base**:
  - **StatefulSet** (3 replicas, `podManagementPolicy: Parallel`) with a per-pod
    `PersistentVolumeClaim` for RocksDB at `/var/kafka-streams`. A restarted pod resumes from its
    local state instead of replaying every changelog, and keeps a stable identity.
  - **Headless Service** `kafka-ledger-headless` gives each pod a stable DNS name, advertised as
    `application.server` via `APP_ADVERTISED_HOST`. `publishNotReadyAddresses: true` keeps peers
    reachable while restoring.
  - **ConfigMap** whose `application-k8s.yml` is mounted at `/config/` (picked up through
    `SPRING_CONFIG_ADDITIONAL_LOCATION`): replication factor 3, 4 stream threads,
    `num.standby.replicas: 1` for fast failover, JWT security on with the identity-provider
    `issuer-uri` (a placeholder to replace).
  - **HPA** on the StatefulSet: CPU only (JVM heap never shrinks, so a memory target blocks
    scale-down), `maxReplicas: 6` = input partition count, and slow, one-pod-at-a-time scaling in
    both directions, because every scale event is a rebalance plus state migration.
  - Service, ServiceMonitor.
- **Dev overlay**: 1 replica, small resources
- **Prod overlay**: 3 replicas, PDB (min 2), pod anti-affinity, larger resources

**Dockerfile**: Multi-stage build with JRE 21 Alpine, non-root user, health check.

```bash
kubectl apply -k k8s/overlays/dev    # Dev
kubectl apply -k k8s/overlays/prod   # Production
```

---

## 19. Terraform (Infrastructure as Code)

`terraform/` provisions all 13 Kafka topics (including `ledger-dlq`) with partition counts, replication, and retention policies.

```bash
cd terraform
terraform init
terraform apply -var="bootstrap_servers=localhost:9092"
```

---

## 20. Testing Strategy

**40 tests** across 9 test classes, none of which need a broker:

| Test Class | Tests | Verifies |
|------------|-------|----------|
| `LedgerTopologyTest` | 8 | Balance aggregation, exact decimal arithmetic, `eventId` dedup (and per-tenant scoping), currency mismatch → `ledger-dlq`, fraud threshold, burst detection on historical event time, stream-time eviction via expiry index, metrics |
| `PaymentStateMachineTest` | 7 | All transitions, reversals, invalid states |
| `PaymentTopologyTest` | 3 | Full lifecycle, reversal, DLQ routing, metrics |
| `PaymentWindowedJoinTopologyTest` | 2 | Window match, no-match for different keys |
| `RevenueAggregationTopologyTest` | 4 | Hourly aggregation, tenant isolation, per-currency separation, real window bounds from the store |
| `SyncRunCdcTopologyTest` | 1 | CDC parent + child merge |
| `MoneyTest` | 5 | Scale normalisation, no rounding, precision limit, ISO currency validation |
| `StreamsDeserializationErrorHandlerTest` | 2 | Poison pill dead-lettered with diagnostics; `FAIL` when the DLQ write fails |
| `ApiSecurityAndValidationTest` | 8 | `401` without token, `403` cross-tenant read/write, `202` only after ack, `503` on failed publish, `400` on invalid bodies and unknown commands (`@WebMvcTest`) |

Topology tests use `TopologyTestDriver` with the same event-time `TimestampExtractor` as
production. Not yet covered: interactive-query forwarding between instances and an end-to-end
run against a real broker (a Testcontainers integration test is the natural next step).

```bash
mvn test
```

---

## 21. Project File Map

```
src/main/avro/                      5 Avro schemas
src/main/java/.../api/              6 REST controllers + RFC 7807 exception handler
src/main/java/.../config/           9 classes (Streams/topic config, RocksDB, error handlers, metrics, health)
src/main/java/.../model/           14 domain records/POJOs + Money/Identifiers rules
src/main/java/.../security/         2 classes (JWT resource server, tenant access check)
src/main/java/.../serde/            1 generic JSON serde
src/main/java/.../streams/         15 topology factories, processors, query routing
src/main/resources/                 application.yml + logback-spring.xml
src/test/java/                      9 test classes (40 tests)
connect/                            Debezium configs + Postgres init SQL
k8s/                                Kubernetes manifests (Kustomize)
terraform/                          Topic provisioning IaC
Dockerfile                          Multi-stage JDK 21 build
docker-compose.yml                  Full Confluent stack + Postgres
docs/                               Documentation collection
```

---

## 22. How to Run

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Build & test
mvn clean test

# 3. Run application (local application.yml: security disabled, single instance)
mvn spring-boot:run

# 4. Submit a transaction
curl -X POST http://localhost:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","tenantId":"tenant-a","accountId":"acct-1",
       "amount":1000,"direction":"CREDIT","currency":"USD","eventTime":1710000000000}'

# 5. Query balance
curl http://localhost:8080/api/ledger/tenant-a/acct-1

# 6. Payment lifecycle
curl -X POST http://localhost:8080/api/payments \
  -H 'Content-Type: application/json' \
  -d '{"paymentId":"pay-1","tenantId":"tenant-a","accountId":"acct-1",
       "amount":250,"currency":"USD","commandType":"INITIATE","eventTime":1710000001000}'

curl http://localhost:8080/api/payments/tenant-a/pay-1

# 7. Revenue query
curl "http://localhost:8080/api/revenue/tenant-a?currency=USD&from=0&to=9999999999999"

# 7b. Replaying the same eventId does not change the balance
curl -X POST http://localhost:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","tenantId":"tenant-a","accountId":"acct-1",
       "amount":1000,"direction":"CREDIT","currency":"USD","eventTime":1710000000000}'
curl http://localhost:8080/api/ledger/tenant-a/acct-1   # still 1000.0000

# 8. Prometheus metrics
curl http://localhost:8080/actuator/prometheus | grep -E "ledger|payment|fraud|dlq"
```

