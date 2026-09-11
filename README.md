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
| **Payment Orchestration** | Full lifecycle state machine (INITIATE→AUTHORIZE→CAPTURE→SETTLE/REVERSE) with DLQ |
| **Fraud Detection** | Burst-window + single-amount threshold rules |
| **Avro + Schema Registry** | 5 Avro schemas, auto-registration, `SpecificAvroSerde` |
| **Kafka Connect (Debezium)** | PostgreSQL CDC for `sync_runs` + `sync_run_payloads`, merged via KTable join |
| **Interactive Queries** | 7 REST endpoints querying RocksDB state stores (KeyValue + WindowStore) |
| **Multi-Tenant SaaS** | `TenantAwarePartitioner` + `TenantRoutingInterceptor` |
| **Exactly-Once Semantics** | `exactly_once_v2` processing guarantee |
| **RocksDB Tuning** | Custom `RocksDBConfigSetter` — 64 MB cache, bloom filters, LZ4, universal compaction |
| **Error Handling / DLQ** | `DeserializationExceptionHandler`, `ProductionExceptionHandler`, payment-dlq branching |
| **Prometheus Metrics** | Micrometer + custom business counters |
| **Kubernetes** | Deployment, HPA, PDB, ServiceMonitor, Kustomize overlays (dev/prod) |
| **Terraform** | IaC for all 13 Kafka topics |
| **Testing** | 17 topology tests with `TopologyTestDriver` |

## Quick Start

```bash
# Start infrastructure
docker compose up -d

# Build & test (17 tests)
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
src/main/java/.../api/              6 REST controllers
src/main/java/.../config/           9 configuration classes (RocksDB, DLQ, metrics, partitioner)
src/main/java/.../model/           12 domain records
src/main/java/.../streams/         13 topology factories + processors + query service
src/test/java/                      6 test classes (17 tests)
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
curl "http://localhost:8080/api/revenue/tenant-a?from=0&to=9999999999999"
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
