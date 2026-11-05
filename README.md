# Kafka Ledger Learning Platform

Production-grade Spring Boot + Kafka Streams project covering every aspect of a
senior Kafka engineer role: stateful streaming, financial logic, payment
orchestration, fraud detection, multi-tenant SaaS, and cloud-native deployment.

## Features

| Area | What's Implemented |
|------|--------------------|
| **Kafka Streams DSL** | Balance aggregation, payment state machine, revenue windows |
| **Processor API** | `FraudDetectionProcessor` — `FixedKeyProcessor` + `Punctuator` |
| **Windowed Joins** | KStream–KStream join matching INITIATED ↔ CAPTURED payments |
| **Tumbling Windows** | Hourly revenue aggregation per tenant |
| **Idempotent Ledger** | Repartition by account + windowed `eventId` dedup store; single-currency accounts, mismatches → `ledger-dlq` |
| **Exact Money** | Avro `decimal(19,4)` / `BigDecimal` end to end, validated at the API boundary |
| **Payment Orchestration** | Full lifecycle state machine (INITIATE→AUTHORIZE→CAPTURE→SETTLE/REVERSE) with DLQ |
| **Fraud Detection** | Burst-window + single-amount rules; stream-time punctuator evicts idle accounts via an expiry index (no full scans) |
| **Avro + Schema Registry** | 5 Avro schemas, auto-registration, `SpecificAvroSerde` |
| **Kafka Connect (Debezium)** | PostgreSQL CDC for `sync_runs` + `sync_run_payloads`, merged via KTable join |
| **Interactive Queries** | REST endpoints over RocksDB stores (KeyValue + WindowStore), routed to the owning instance via `application.server` metadata |
| **Multi-Tenant SaaS** | JWT `tenant_id` claim enforced on every read/write (incl. forwarded queries), `tenantId\|subKey` keys with validated IDs, `X-Tenant-Id` header interceptor |
| **Exactly-Once Semantics** | `exactly_once_v2` processing guarantee |
| **RocksDB Tuning** | Custom `RocksDBConfigSetter` — 64 MB cache, bloom filters, LZ4, universal compaction |
| **Error Handling / DLQ** | Poison pills → `dlq-deserialization-errors` with diagnostics (FAIL if the DLQ write fails); production errors FAIL; client shutdown → liveness DOWN; business DLQs `payment-dlq`, `ledger-dlq` |
| **Prometheus Metrics** | Micrometer + business counters (processed, duplicates, transitions by status, alerts by reason, DLQ by topic) |
| **Kubernetes** | StatefulSet with per-pod state volumes, headless service, standby replicas, damped HPA, PDB, ServiceMonitor, Kustomize overlays |
| **Terraform** | IaC for all 13 Kafka topics |
| **Testing** | 40 tests: topologies (`TopologyTestDriver`, production event-time extractor), API auth/validation/ack semantics (`@WebMvcTest`), DLQ handler, money rules |

## Quick Start

```bash
# Start infrastructure
docker compose up -d

# Build & test
mvn clean test

# Run application
mvn spring-boot:run
```

## Documentation

| Document | Purpose |
|----------|---------|
| **[docs/comprehensive-reference.md](docs/comprehensive-reference.md)** | Complete reference — all 6 topologies, every component, configuration, and design decision |
| **[docs/event-catalog.md](docs/event-catalog.md)** | All topics, schemas, state stores, and REST endpoints |
| **[QUICKSTART.md](QUICKSTART.md)** | 20-minute environment verification |
| **[LEARNING-PATH.md](LEARNING-PATH.md)** | Curriculum guide + self-assessment |
| **[docs/testing-playbook.md](docs/testing-playbook.md)** | 14 hands-on labs |
| **[docs/advanced-guide.md](docs/advanced-guide.md)** | Senior-level deep dive |
| **[docs/application-architecture-and-kafka-theory.md](docs/application-architecture-and-kafka-theory.md)** | Original architecture + Kafka theory reference |

## Project Layout

```
src/main/avro/                      5 Avro schemas
src/main/java/.../api/              6 REST controllers + RFC 7807 exception handler
src/main/java/.../config/           9 classes (Streams/topic config, RocksDB, error handlers, metrics, health)
src/main/java/.../model/           14 domain records + Money/Identifiers rules
src/main/java/.../security/         JWT resource server + tenant access check
src/main/java/.../streams/         15 topology factories, processors, query routing
src/test/java/                      9 test classes (40 tests)
connect/                            Debezium configs + Postgres init SQL
k8s/                                Kubernetes manifests (Kustomize base + dev/prod overlays)
terraform/                          Topic provisioning IaC
Dockerfile                          Multi-stage JDK 21 build
docker-compose.yml                  Full Confluent stack + Postgres
```

## Prerequisites

- Java 21
- Maven 3.9+
- Docker Desktop

## Infrastructure URLs

| Service | URL | Notes |
|---------|-----|-------|
| Kafka broker | `localhost:9092` | Kafka protocol (not HTTP) |
| Schema Registry | `http://localhost:8081` | REST API |
| Kafka Connect | `http://localhost:8083` | REST API |
| Kafka UI | `http://localhost:8085` | Web dashboard |
| Postgres | `localhost:5433` | CDC source database |

## Try It

> Local `application.yml` sets `app.security.enabled=false` so these curls work without an
> identity provider. The k8s profile enables JWT auth: every `/api` call then needs
> `Authorization: Bearer <token>` with a `tenant_id` claim matching the tenant in the request.
> Writes return `202` only after the broker acknowledges them (`503` + `Retry-After` otherwise).

### Ledger

```bash
# Submit a transaction
curl -X POST http://localhost:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","tenantId":"tenant-a","accountId":"acct-1",
       "amount":1000,"direction":"CREDIT","currency":"USD","eventTime":1710000000000}'

# Query balance
curl http://localhost:8080/api/ledger/tenant-a/acct-1

# Query fraud state
curl http://localhost:8080/api/fraud/tenant-a/acct-1
```

### Payment Orchestration

```bash
# Initiate → Authorize → Capture → Settle
for CMD in INITIATE AUTHORIZE CAPTURE SETTLE; do
  curl -s -X POST http://localhost:8080/api/payments \
    -H 'Content-Type: application/json' \
    -d "{\"paymentId\":\"pay-1\",\"tenantId\":\"tenant-a\",\"accountId\":\"acct-1\",
         \"amount\":250,\"currency\":\"USD\",\"commandType\":\"$CMD\",\"eventTime\":$(date +%s)000}"
  sleep 1
done

# Query payment state
curl http://localhost:8080/api/payments/tenant-a/pay-1

# Query hourly revenue (after SETTLE)
curl "http://localhost:8080/api/revenue/tenant-a?currency=USD&from=0&to=9999999999999"
```

### Debezium CDC

```bash
# Register connector
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connect/debezium-sync-runs-source.json

# Insert data
docker compose exec postgres psql -U user -d sync -c \
  "INSERT INTO sync_runs (status, created_at, sync_job_id) VALUES ('STARTED', NOW(), 'job-1');"

# Query merged state
SYNC_ID=$(docker compose exec -T postgres psql -U user -d sync -At -c \
  "SELECT sync_run_id FROM sync_runs LIMIT 1;")
curl "http://localhost:8080/api/sync-runs/$SYNC_ID"
```

### Prometheus Metrics

```bash
curl http://localhost:8080/actuator/prometheus | grep -E "ledger|payment|fraud|dlq"
```
