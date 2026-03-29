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
                           │     RocksDB State Stores (7 stores)        │
                           └────────┼──────────────┼─────────────┼──────┘
                                    │              │             │
                           ─────────┴──────────────┴─────────────┴──────
                                         Apache Kafka
                              (13 topics · Schema Registry · Connect)
```

### Data Flow Summary

| Flow | Source | Topology | Output Topic | State Store |
|------|--------|----------|--------------|-------------|
| Ledger | `tx-events` | `LedgerTopologyFactory` | `ledger-balances` | `ledger-balance-store` |
| Fraud | `tx-events` | `FraudDetectionProcessor` | `fraud-alerts` | `fraud-activity-store` |
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

Consumes `TransactionEvent` messages from `tx-events`, re-keys them by
`tenantId|accountId`, and aggregates a running balance in a KTable
materialised to the `ledger-balance-store`.

### How Balance Is Calculated

```java
double signedAmount = event.getDirection() == TxDirection.DEBIT
        ? -Math.abs(event.getAmount())  // Debits subtract
        : Math.abs(event.getAmount());  // Credits add

newBalance = currentBalance + signedAmount;
```

The aggregate is initialised at `balance = 0.0` for each new key. Every event
updates the balance and writes the new `LedgerBalance` snapshot to both the
state store and the `ledger-balances` output topic (compacted).

### Topology Diagram

```
tx-events (Avro)
    │
    ▼  selectKey → tenantId|accountId
    │
    ▼  groupByKey → aggregate(balance + signed amount)
    │         materialised as → ledger-balance-store (RocksDB)
    │
    ▼  toStream
    │
    ▼  ledger-balances (Avro, compacted)
```

### State Store

| Store | Type | Key | Value |
|-------|------|-----|-------|
| `ledger-balance-store` | Persistent KeyValue | `tenantId\|accountId` | `LedgerBalance` (Avro) |

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

The older `Transformer` API (deprecated in Kafka 3.3) is preserved in
`FraudDetectorTransformer.java` for reference but is no longer used in the topology.

### Fraud Rules

| Rule | Trigger | Alert Reason |
|------|---------|--------------|
| Single-amount threshold | `abs(amount) >= 10,000` | `SINGLE_TX_THRESHOLD_EXCEEDED` |
| Burst activity | `≥ 5 events in 60 seconds` | `BURST_ACTIVITY_DETECTED` |

Both thresholds are configurable via `LedgerStreamProperties` (`app.ledger.*`
in `application.yml`).

### Punctuator (Scheduled Processing)

A wall-clock `Punctuator` runs every **30 seconds** and iterates the entire
`fraud-activity-store`. Any entry whose `lastEventTime` is older than
`2 × burstWindowMs` is evicted — preventing unbounded state growth.

```java
context.schedule(
    Duration.ofSeconds(30),
    PunctuationType.WALL_CLOCK_TIME,
    this::expireStaleWindows
);
```

### How It Integrates with the DSL

```java
events.<FraudAlert>processValues(
    () -> new FraudDetectionProcessor(properties),
    Named.as("fraud-detector"),
    Topics.FRAUD_ACTIVITY_STORE
)
.filter((key, alert) -> alert != null)
.to(Topics.FRAUD_ALERTS, Produced.with(stringSerde, fraudAlertSerde));
```

`processValues()` preserves the key while the processor decides whether to
forward a `FraudAlert` or nothing. Only non-null alerts pass the downstream
filter and reach the `fraud-alerts` topic.

### State Store

| Store | Type | Key | Value |
|-------|------|-----|-------|
| `fraud-activity-store` | Persistent KeyValue | `tenantId\|accountId` | `AccountActivity` (JSON) |

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

1. **REST API** (`POST /api/payments`) receives a `PaymentRequest`, converts it to an Avro `PaymentEvent`, and produces to `payment-commands`.
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
    double amount, String currency, String status, String failReason,
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

Computes **hourly revenue per tenant** using tumbling time windows on settled
payments.

### How It Works

1. Reads from `payment-events`, filters for `status == SETTLED`.
2. Re-keys by `tenantId`.
3. Applies `TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1))`.
4. Aggregates `totalAmount` and `transactionCount` within each window.
5. Materialises into a **WindowStore** (`revenue-hourly-store`).
6. Embeds window boundaries from the `Windowed<String>` key into the output `TenantRevenue`.

```
payment-events
    │
    ▼  filter(SETTLED) → selectKey(tenantId)
    │
    ▼  windowedBy(1-hour tumbling) → aggregate(sum, count)
    │     materialised as → revenue-hourly-store (WindowStore)
    │
    ▼  tenant-revenue-hourly (Avro)
```

### Windowed Interactive Query

The `LedgerQueryService.findHourlyRevenue()` method queries the WindowStore:

```java
ReadOnlyWindowStore<String, TenantRevenue> store = kafkaStreams.store(
    StoreQueryParameters.fromNameAndType(Topics.REVENUE_STORE, QueryableStoreTypes.windowStore())
);
WindowStoreIterator<TenantRevenue> iter = store.fetch(tenantId, fromInstant, toInstant);
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

**File:** `LedgerQueryService.java`

Interactive queries read directly from local RocksDB state stores for
**sub-millisecond latency** without hitting Kafka brokers.

| Method | Path | Store | Response |
|--------|------|-------|----------|
| `POST` | `/api/transactions` | — | Produces `TransactionEvent` |
| `POST` | `/api/payments` | — | Produces `PaymentEvent` |
| `GET` | `/api/ledger/{tenantId}/{accountId}` | `ledger-balance-store` | `BalanceView` |
| `GET` | `/api/fraud/{tenantId}/{accountId}` | `fraud-activity-store` | `FraudStateView` |
| `GET` | `/api/payments/{tenantId}/{paymentId}` | `payment-state-store` | `PaymentState` |
| `GET` | `/api/revenue/{tenantId}?from=&to=` | `revenue-hourly-store` | `List<RevenueWindowEntry>` |
| `GET` | `/api/sync-runs/{syncRunId}` | `sync-runs-merged-store` | `SyncRunMergedRecord` |

---

## 11. Multi-Tenant SaaS Architecture

### Key Convention

All composite keys follow `tenantId|subKey` — e.g. `tenant-a|acct-1` or `tenant-a|pay-1`.

### TenantAwarePartitioner (`config/TenantAwarePartitioner.java`)

Custom `StreamPartitioner` that hashes **only on `tenantId`**:
- All accounts for a tenant are co-located on the same partition.
- KTable joins and aggregations work correctly across sub-keys.
- Tenant isolation for processing.

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

| Layer | Handler | Behaviour |
|-------|---------|-----------|
| Deserialization | `StreamsDeserializationErrorHandler` | Log + skip |
| Production | `StreamsProductionExceptionHandler` | Log + continue |
| Business logic | `PaymentTopologyFactory` DLQ branch | Route to `payment-dlq` |

`DeadLetterQueueHandler.enrichHeaders()` adds `dlq.original.topic`, `dlq.error.message`, `dlq.error.stacktrace` headers.

---

## 15. Serialization Strategy

| Approach | Used For | Why |
|----------|----------|-----|
| **Avro + Schema Registry** | Financial events (`TransactionEvent`, `PaymentEvent`, `LedgerBalance`, `FraudAlert`, `TenantRevenue`) | Schema evolution, type safety |
| **JSON (`JsonSerde`)** | State stores (`PaymentState`, `AccountActivity`), CDC records | Flexibility, no registry dependency |

---

## 16. Observability & Metrics

- **Prometheus** at `/actuator/prometheus` via `micrometer-registry-prometheus`.
- Custom counters: `ledger_transactions_processed`, `payments_state_transitions`, `fraud_alerts_emitted`, `dlq_records`.
- **Structured logging**: JSON for `k8s`/`prod` profiles, human-readable for `default`.
- **Health checks**: `/actuator/health` with liveness/readiness probes.

---

## 17. Configuration Reference

See `src/main/resources/application.yml` for all settings. Key properties:

- `processing.guarantee: exactly_once_v2` — EOS
- `rocksdb.config.setter: RocksDbConfig` — custom tuning
- `default.timestamp.extractor: TransactionEventTimestampExtractor` — event-time
- `default.deserialization.exception.handler` — poison pill handling
- `interceptor.classes: TenantRoutingInterceptor` — tenant header enrichment

---

## 18. Kubernetes Deployment

`k8s/` uses **Kustomize** with base + overlay pattern:

- **Base**: Deployment (3 replicas), Service, ConfigMap, HPA (2–12 pods), ServiceMonitor
- **Dev overlay**: 1 replica, small resources
- **Prod overlay**: 3 replicas, PDB (min 2), pod anti-affinity, larger resources

**Dockerfile**: Multi-stage build with JRE 21 Alpine, non-root user, health check.

```bash
kubectl apply -k k8s/overlays/dev    # Dev
kubectl apply -k k8s/overlays/prod   # Production
```

---

## 19. Terraform (Infrastructure as Code)

`terraform/` provisions all 13 Kafka topics with partition counts, replication, and retention policies.

```bash
cd terraform
terraform init
terraform apply -var="bootstrap_servers=localhost:9092"
```

---

## 20. Testing Strategy

**17 tests** across 6 test classes using `TopologyTestDriver` (no broker needed):

| Test Class | Tests | Verifies |
|------------|-------|----------|
| `LedgerTopologyTest` | 2 | Balance aggregation, fraud alert threshold |
| `PaymentStateMachineTest` | 7 | All transitions, reversals, invalid states |
| `PaymentTopologyTest` | 3 | Full lifecycle, reversal, DLQ routing |
| `PaymentWindowedJoinTopologyTest` | 2 | Window match, no-match for different keys |
| `RevenueAggregationTopologyTest` | 2 | Hourly aggregation, tenant isolation |
| `SyncRunCdcTopologyTest` | 1 | CDC parent + child merge |

```bash
mvn test   # All 17 tests pass
```

---

## 21. Project File Map

```
src/main/avro/                      5 Avro schemas
src/main/java/.../api/              6 REST controllers
src/main/java/.../config/           9 configuration classes
src/main/java/.../model/           12 domain records/POJOs
src/main/java/.../serde/            1 generic JSON serde
src/main/java/.../streams/         13 topology factories + processors + services
src/main/resources/                 application.yml + logback-spring.xml
src/test/java/                      6 test classes (17 tests)
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

# 2. Build & test (17 tests)
mvn clean test

# 3. Run application
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
curl "http://localhost:8080/api/revenue/tenant-a?from=0&to=9999999999999"

# 8. Prometheus metrics
curl http://localhost:8080/actuator/prometheus | grep -E "ledger|payment|fraud|dlq"
```

