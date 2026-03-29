# Senior Kafka Learning and Testing Playbook

This guide is a structured practice path to help you test and experiment with the skills from your job description using this project.

It is organized as labs. Each lab has:

- goal (what skill you are practicing)
- commands (what to run)
- expected result (what should happen)
- stretch tasks (how to go deeper)

## 0) Skill-to-Lab Map

Use this map to connect the role requirements to concrete project exercises.

- Java + Spring Boot + Kafka Streams
  - Labs 1, 2, 3, 4
- Stateful streaming, state stores, interactive queries
  - Labs 3, 4, 8
- Exactly-once semantics (EOS)
  - Labs 5, 9
- Schema Registry + schema evolution
  - Labs 6, 12
- Kafka Connect
  - Lab 7
- Financial logic (ledgering, fraud)
  - Labs 2, 3, 10
- Multi-tenant SaaS patterns
  - Lab 8
- High availability, low latency, resilience
  - Labs 9, 11
- Leadership/mentoring mindset
  - Lab 13
- Cloud/K8s/Terraform readiness
  - Lab 14

## 1) Environment Smoke Test

### Goal

Verify local environment and run baseline tests.

### Commands

```bash
cd /Users/bojan/IdeaProjects/java-kafka
```

```bash
docker compose up -d
```

```bash
docker compose ps
```

```bash
open http://localhost:8085
```

```bash
curl -s http://localhost:8081/subjects
```

```bash
mvn test
```

```bash
mvn spring-boot:run
```

### Expected result

- `kafka`, `schema-registry`, `connect`, and `kafka-ui` are healthy in `docker compose ps`.
- `curl` to Schema Registry returns JSON (likely `[]` at start).
- Kafka UI opens at `http://localhost:8085`.
- Maven tests pass.
- App starts and exposes REST endpoints.

**Port reminder:**
- `9092` is Kafka broker traffic, not a browser UI
- `9101` is Kafka JMX, not a browser UI
- `8085` is the Kafka browser UI

### Stretch

- Add Maven Wrapper (`./mvnw`) so teammates do not need local Maven.

---

## 2) Core Financial Ledger Correctness

### Goal

Validate source-of-truth event handling and signed balance logic.

### Commands

Post credit:

```bash
curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-ledger-1","tenantId":"tenant-a","accountId":"acct-1","amount":1000.0,"direction":"CREDIT","currency":"USD","eventTime":1710000000000}'
```

Post debit:

```bash
curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-ledger-2","tenantId":"tenant-a","accountId":"acct-1","amount":250.0,"direction":"DEBIT","currency":"USD","eventTime":1710000001000}'
```

Query state:

```bash
curl http://localhost:8080/api/ledger/tenant-a/acct-1
```

### Expected result

- Final balance is `750.0` (credit minus debit).
- `lastEventTime` reflects second event time.

### Stretch

- Add negative amount protection in API validation.
- Add currency consistency rule (reject mixed-currency account streams).

---

## 3) Fraud Detection (Processor API + Stateful)

### Goal

Exercise Processor API transformer + state store behavior for threshold and burst detection.

### Commands

Single high-value transaction:

```bash
curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-fraud-1","tenantId":"tenant-a","accountId":"acct-risk","amount":25000.0,"direction":"CREDIT","currency":"USD","eventTime":1710000010000}'
```

Read fraud state:

```bash
curl http://localhost:8080/api/fraud/tenant-a/acct-risk
```

Create burst activity (repeat quickly with different event IDs):

```bash
for i in 1 2 3 4 5; do curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d "{\"eventId\":\"evt-burst-$i\",\"tenantId\":\"tenant-a\",\"accountId\":\"acct-burst\",\"amount\":10.0,\"direction\":\"CREDIT\",\"currency\":\"USD\",\"eventTime\":1710000020${i}00}"; done
```

### Expected result

- High-value event produces alert path in `fraud-alerts`.
- Fraud state endpoint returns increased `eventsInWindow` for burst account.

### Stretch

- Add a rolling amount-in-window feature and trigger a third fraud reason.
- Store risk score in a dedicated state store.

---

## 4) Interactive Queries and Read Path Contracts

### Goal

Validate low-latency reads from state stores and define API contracts.

### Commands

```bash
curl http://localhost:8080/api/ledger/tenant-a/acct-1
```

```bash
curl http://localhost:8080/api/fraud/tenant-a/acct-burst
```

Non-existing account:

```bash
curl -i http://localhost:8080/api/ledger/tenant-a/not-found
```

### Expected result

- Existing accounts return `200` with state snapshot.
- Missing account returns `404`.

### Stretch

- Add response versioning (`/v1/ledger/...`) and OpenAPI docs.
- Add endpoint latency SLO (p95 under target) and monitor via Actuator/Micrometer.

---

## 5) Exactly-Once Semantics (EOS) Verification

### Goal

Understand and test transactional consistency guarantees.

Current config uses `processing.guarantee: exactly_once_v2` in `src/main/resources/application.yml`.

### Commands

Observe processing guarantee in effective config logs:

```bash
grep -n "processing.guarantee" /Users/bojan/IdeaProjects/java-kafka/src/main/resources/application.yml
```

Produce a repeatable test sequence (same event payload multiple times):

```bash
for i in 1 2 3; do curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-eos-dup","tenantId":"tenant-eos","accountId":"acct-1","amount":100.0,"direction":"CREDIT","currency":"USD","eventTime":1710000030000}'; done
```

Query balance:

```bash
curl http://localhost:8080/api/ledger/tenant-eos/acct-1
```

### Expected result

- You should see deterministic processing behavior with transactional stream writes.
- Note: this project does not yet implement explicit deduplication by `eventId`, so replaying same event ID can still increase business balance if treated as independent events.

### Stretch

- Implement event-id dedup state store and compare behavior before/after.
- Add integration test asserting no double-application for duplicate event IDs.

---

## 6) Schema Registry and Avro Evolution

### Goal

Practice schema governance and compatibility checks.

### Commands

List subjects:

```bash
curl -s http://localhost:8081/subjects
```

Inspect versions for a subject (replace with actual generated subject):

```bash
curl -s http://localhost:8081/subjects/tx-events-value/versions
```

Check compatibility mode globally:

```bash
curl -s http://localhost:8081/config
```

### Expected result

- Subjects appear after producers/streams write Avro data.
- You can inspect registered schema versions.

### Stretch

- Add optional field in `TransactionEvent.avsc` with default and verify backward compatibility.
- Try an intentionally breaking change (remove field without default) and observe registry rejection when compatibility enforces it.

---

## 7) Kafka Connect Practice

### Goal

Learn connector lifecycle and integration touchpoints.

### Commands

Register file source:

```bash
curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @/Users/bojan/IdeaProjects/java-kafka/connect/ledger-source.json
```

Register file sink:

```bash
curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @/Users/bojan/IdeaProjects/java-kafka/connect/ledger-sink.json
```

Register Debezium Postgres CDC source (`sync.sync_runs`):

```bash
curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @/Users/bojan/IdeaProjects/java-kafka/connect/debezium-sync-runs-source.json
```

List connectors:

```bash
curl -s http://localhost:8083/connectors
```

Get connector status:

```bash
curl -s http://localhost:8083/connectors/sync-runs-postgres-cdc/status
```

### Expected result

- Connect REST API shows running connectors/tasks.
- Debezium connector emits table change events to topics `sync.public.sync_runs` and `sync.public.sync_run_payloads`.
- Kafka Streams merges those CDC events into `sync-runs-enriched` keyed by `sync_run_id`.
- You can iterate on connector configs and observe restart behavior.

### End-to-end CDC merge check

Apply the SQL bootstrap to an existing Postgres volume:

```bash
docker compose exec -T postgres psql -U user -d sync -v ON_ERROR_STOP=1 < /Users/bojan/IdeaProjects/java-kafka/connect/postgres-init-sync.sql
```

Insert one sync run and one payload:

```bash
docker compose exec postgres psql -U user -d sync -c "INSERT INTO public.sync_runs (sync_run_id, status, created_at, created_by, sync_job_id, job_info) VALUES ('run-lab-1', 'STARTED', NOW(), 'lab-user', 'job-lab-1', 'lab run');"
```

```bash
docker compose exec postgres psql -U user -d sync -c "INSERT INTO public.sync_run_payloads (sync_run_payload_id, sync_run_id, transaction_id, order_number, order_amount, created_at, total_items, note) VALUES ('payload-lab-1', 'run-lab-1', 'tx-lab-1', 'ord-lab-1', 88.25, NOW(), 3, 'payload row');"
```

Consume the merged topic:

```bash
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic sync-runs-enriched --from-beginning
```

Expected merged shape:

- one document per `sync_run_id`
- embedded `syncRun` object from `public.sync_runs`
- `payloads` array with all currently known child payload rows
- `payloadCount` matching the array size

### Stretch

- Replace file connectors with JDBC Source + Sink.
- Move Debezium output from JSON to Avro converter + Schema Registry.

---

## 8) Multi-Tenant SaaS Isolation and Scaling

### Goal

Validate tenant isolation and partitioning strategy.

### Commands

Send transactions for two tenants same account ID:

```bash
curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-mt-1","tenantId":"tenant-x","accountId":"acct-1","amount":500.0,"direction":"CREDIT","currency":"USD","eventTime":1710000040000}'
```

```bash
curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-mt-2","tenantId":"tenant-y","accountId":"acct-1","amount":800.0,"direction":"CREDIT","currency":"USD","eventTime":1710000041000}'
```

Query each:

```bash
curl http://localhost:8080/api/ledger/tenant-x/acct-1
```

```bash
curl http://localhost:8080/api/ledger/tenant-y/acct-1
```

### Expected result

- No cross-tenant contamination in balances/fraud state.
- Keys are tenant-scoped (`tenantId|accountId`).

### Stretch

- Simulate hot-tenant traffic and evaluate lag/latency.
- Add tenant quotas and backpressure policy.

---

## 9) Failure and Recovery Labs

### Goal

Test resilience, recovery, and state rebuild behavior.

### Commands

Stop app while infrastructure keeps running, then restart app.

```bash
pkill -f "spring-boot:run"
```

```bash
cd /Users/bojan/IdeaProjects/java-kafka
```

```bash
mvn spring-boot:run
```

Restart Kafka container:

```bash
docker compose restart kafka
```

### Expected result

- Streams app reconnects and resumes processing.
- State stores restore from changelog where needed.

### Stretch

- Measure time to full restoration under larger state volume.
- Add readiness checks that block traffic until stores are queryable.

---

## 10) Financial Integrity Scenarios

### Goal

Practice realistic finance-engine behaviors.

### Scenarios to implement and test

- idempotent payment command handling
- reversal/refund events
- settlement windows and cut-off times
- overdraft or balance-floor enforcement
- out-of-order event tolerance policy

### Suggested tests

- unit tests for pure financial rules
- topology tests for sequence correctness
- integration tests for API-to-topic-to-state flow

---

## 11) Performance and RocksDB Tuning Labs

### Goal

Understand state-store performance and tuning levers.

### Commands

Generate load (simple loop):

```bash
for i in $(seq 1 2000); do curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d "{\"eventId\":\"evt-load-$i\",\"tenantId\":\"tenant-load\",\"accountId\":\"acct-$((i%20))\",\"amount\":1.0,\"direction\":\"CREDIT\",\"currency\":\"USD\",\"eventTime\":171000005${i}}" >/dev/null; done
```

Inspect app metrics endpoint:

```bash
curl -s http://localhost:8080/actuator/metrics
```

### Expected result

- App remains responsive and query endpoints still return data.
- Metrics surface useful counters/timers for tuning experiments.

### Stretch

- Tune cache size and commit interval in streams properties.
- Add explicit RocksDB config via `RocksDBConfigSetter` and compare write/read behavior.

---

## 12) Contract and Compatibility CI Gate (Recommended)

### Goal

Introduce governance expected in senior roles.

### What to add

- schema compatibility check in CI
- topic contract tests (producer + consumer)
- forbidden breaking change checks for financial events

### Expected result

- Pull requests fail early on schema or contract regressions.

---

## 13) Leadership and Mentoring Exercises

### Goal

Practice staff-level responsibilities beyond coding.

### Exercises

- Write an ADR for key design choices (`tenantId|accountId` key, EOS mode, compacted balances topic).
- Run a topology design review with trade-offs (DSL vs Processor API split).
- Define incident response playbook for high-latency or duplicate-processing incidents.
- Mentor exercise: ask a junior engineer to add a fraud rule; review for correctness, test coverage, and operability.

### Expected result

- Clear, communicative engineering artifacts and review standards.

---

## 14) Cloud/K8s/Terraform Readiness Track

### Goal

Prepare for production-like deployment and operations.

### Suggested progression

1. Containerize app with health probes.
2. Deploy Kafka-compatible cluster (managed or self-hosted).
3. Deploy app on Kubernetes with resource requests/limits and rolling updates.
4. Define Terraform modules for infra and environment promotion.
5. Add dashboards/alerts for lag, throughput, error rates, state-restore times.

### Expected result

- Repeatable environment setup and observable runtime behavior.

---

## 15) Suggested Weekly Progression (6 Weeks)

- Week 1: Labs 1-3 (foundations + correctness)
- Week 2: Labs 4-6 (queries + EOS + schema)
- Week 3: Labs 7-9 (connect + multi-tenant + resilience)
- Week 4: Labs 10-11 (financial integrity + performance)
- Week 5: Labs 12-13 (governance + leadership)
- Week 6: Lab 14 + production-style runbook draft

## 16) Evidence Checklist (Portfolio Ready)

Track these artifacts to demonstrate senior readiness:

- topology diagrams and ADRs
- benchmark notes and tuning diffs
- schema evolution examples with compatibility results
- incident/failure test reports
- mentoring notes and code review rubrics
- test matrix (unit, topology, integration, failure)

## 17) Notes and Current Project Boundaries

- `exactly_once_v2` is configured, but business-level deduplication by `eventId` is not yet implemented.
- Connect configs are starter examples and currently use file connectors.
- This project is ideal for local learning and extension into production-grade patterns.

---

## 18) Troubleshooting Guide

Common issues and how to resolve them.

### Issue: Schema Registry cannot connect to Kafka

**Symptom**

```
[ERROR] Expected 1 brokers but found only 0
Connection to node 1 could not be established
```

**Root Cause**

- Kafka may still be starting up (takes 5-10 seconds).
- Containers were not fully initialized.
- Docker DNS resolution issue.

**Solution**

1) Restart the full stack with a clean slate:

```bash
cd /Users/bojan/IdeaProjects/java-kafka
docker compose down
docker compose up -d
sleep 5
```

2) Verify all containers are running:

```bash
docker compose ps
```

Expected output: all services with `Up` status.

3) Test connectivity:

```bash
curl -s http://localhost:8081/subjects
```

Should return `[]` (empty list of schemas).

4) If still failing, check container logs:

```bash
docker compose logs schema-registry
docker compose logs kafka
```

### Issue: Kafka broker on port 9092 is not accessible

**Symptom**

```
Connection refused
Failed to connect to localhost:9092
```

**Root Cause**

- Container is running but port mapping is not active.
- Docker networking issue (especially on Mac with Docker Desktop).
- Port 9092 is already in use locally.

**Solution**

1) Check port availability:

```bash
lsof -i :9092
```

If something is using it, kill that process or stop Docker Desktop.

2) Verify Docker Desktop is running:

```bash
docker ps
```

3) Check Kafka container logs for startup errors:

```bash
docker compose logs kafka | tail -50
```

4) Restart only Kafka:

```bash
docker compose restart kafka
sleep 3
docker compose logs kafka | grep -i "started" | tail -5
```

### Issue: Application fails to start with `Streams config` error

**Symptom**

```
org.apache.kafka.streams.errors.StreamsException: org.apache.kafka.common.errors.TimeoutException
```

**Root Cause**

- Kafka cluster is unreachable from the app.
- App is configured with wrong bootstrap server.
- Application starts before Kafka is fully ready.

**Solution**

1) Verify Kafka is healthy:

```bash
docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

2) Check app config points to correct broker:

```bash
grep "bootstrap-servers" /Users/bojan/IdeaProjects/java-kafka/src/main/resources/application.yml
```

Should show: `bootstrap-servers: localhost:9092`

3) Ensure Kafka is ready before starting app (wait 10 seconds after `docker compose up -d`):

```bash
docker compose up -d
sleep 10
mvn spring-boot:run
```

### Issue: `curl` requests to app return `Connection refused`

**Symptom**

```
curl: (7) Failed to connect to localhost port 8080: Connection refused
```

**Root Cause**

- App is not running.
- App is still starting up (Spring Boot takes 10-15 seconds).
- Another service is using port 8080.

**Solution**

1) Check if app is running:

```bash
curl -i http://localhost:8080/actuator/health
```

Wait 15 seconds if app was just started.

2) If no response, restart the app:

```bash
mvn spring-boot:run
```

Watch logs for `Started LedgerLearningApplication`.

3) Check port availability:

```bash
lsof -i :8080
```

### Issue: `mvn test` fails with dependency resolution errors

**Symptom**

```
[ERROR] Failed to execute goal on project kafka-ledger-learning: Could not resolve dependencies
```

**Root Cause**

- Maven dependency cache is corrupted.
- Network access to Maven Central or Confluent repo is blocked.

**Solution**

1) Clear Maven cache:

```bash
rm -rf ~/.m2/repository
```

2) Try again:

```bash
mvn clean test -U
```

The `-U` flag forces update of snapshots.

3) If Confluent repo is unreachable, check your internet connection and network proxies.

### Issue: Docker Compose fails to start containers

**Symptom**

```
ERROR: Service 'kafka' failed to build
```

**Root Cause**

- Docker daemon is not running.
- Docker image pull failed.
- docker-compose.yml syntax error.

**Solution**

1) Ensure Docker is running:

```bash
docker ps
```

If not running, start Docker Desktop (macOS).

2) Validate docker-compose.yml syntax:

```bash
docker compose config
```

3) Force pull fresh images:

```bash
docker compose down
docker compose pull
docker compose up -d
```

4) Check Docker disk space:

```bash
docker system df
```

If disk is full, run:

```bash
docker system prune -a
```

### Issue: State store queries return 404 even after publishing events

**Symptom**

```
curl http://localhost:8080/api/ledger/tenant-a/acct-1
→ 404 Not Found
```

**Root Cause**

- Events have not been processed yet (lag).
- Tenant/account key does not match between publish and query.
- App is not running or topology is not initialized.

**Solution**

1) Verify app is healthy:

```bash
curl http://localhost:8080/actuator/health
```

Should return `UP`.

2) Wait for processing (state stores initialize and process events):

```bash
sleep 5
```

3) Check key format is exact:

When publishing:
```bash
curl -X POST http://localhost:8080/api/transactions ... -d '{"tenantId":"tenant-a","accountId":"acct-1",...}'
```

When querying:
```bash
curl http://localhost:8080/api/ledger/tenant-a/acct-1
```

Both must use identical `tenantId` and `accountId`.

4) Publish a fresh event and immediately check logs:

```bash
# Terminal 1: watch app logs
mvn spring-boot:run | grep -i "ledger\|fraud"

# Terminal 2: publish event
curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-test-123","tenantId":"t1","accountId":"a1","amount":100,"direction":"CREDIT","currency":"USD","eventTime":1710000000000}'

# Terminal 2: query result
sleep 2
curl http://localhost:8080/api/ledger/t1/a1
```

### Issue: Fraud alerts are not triggering

**Symptom**

High-value transactions publish but fraud state endpoint returns 404 or `eventsInWindow: 0`.

**Root Cause**

- Threshold value is different from your test amount.
- Fraud transformer is not receiving events (topology issue).
- State store is not being populated.

**Solution**

1) Check configured threshold:

```bash
grep "single-transaction-alert-threshold" /Users/bojan/IdeaProjects/java-kafka/src/main/resources/application.yml
```

Default is `10000`. Publish an amount higher than this.

2) Post a test transaction exceeding threshold:

```bash
curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-fraud-test","tenantId":"tenant-fraud","accountId":"acct-test","amount":50000.0,"direction":"CREDIT","currency":"USD","eventTime":1710000100000}'
```

3) Query fraud state:

```bash
curl http://localhost:8080/api/fraud/tenant-fraud/acct-test
```

4) If still 404, check app logs for errors in the fraud transformer.

### Issue: Schema Registry subjects are empty after publishing events

**Symptom**

```bash
curl -s http://localhost:8081/subjects
→ []
```

**Root Cause**

- App has not yet published any Avro events.
- Avro serializer is not configured correctly.
- Events are being published with String serializer instead of Avro.

**Solution**

1) Verify app config has Avro producer serializer:

```bash
grep -A5 "producer:" /Users/bojan/IdeaProjects/java-kafka/src/main/resources/application.yml | grep -i serializer
```

Should show: `io.confluent.kafka.serializers.KafkaAvroSerializer`

2) Publish an event and wait 2 seconds:

```bash
curl -X POST http://localhost:8080/api/transactions -H 'Content-Type: application/json' -d '{"eventId":"evt-schema","tenantId":"t1","accountId":"a1","amount":100,"direction":"CREDIT","currency":"USD","eventTime":1710000000000}'
sleep 2
curl -s http://localhost:8081/subjects
```

You should now see subjects like `tx-events-value`, `ledger-balances-value`, etc.

---

## 19) Quick Reference: Common Commands

### Container Management

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# View container status
docker compose ps

# View service logs
docker compose logs <service-name>

# Restart a service
docker compose restart <service-name>
```

### Application

```bash
# Run tests
mvn test

# Start app
mvn spring-boot:run

# Kill app
pkill -f "spring-boot:run"
```

### Kafka/Schema Registry

```bash
# List topics
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describe topic
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic tx-events

# List schema subjects
curl -s http://localhost:8081/subjects

# Check Kafka broker health
docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### API Queries

```bash
# Health check
curl http://localhost:8080/actuator/health

# List available metrics
curl -s http://localhost:8080/actuator/metrics | jq

# Get specific metric
curl -s http://localhost:8080/actuator/metrics/<metric-name>
```

---

## 20) When to Seek Help

If you have tried all troubleshooting steps and issues persist:

1. Gather diagnostic data:

```bash
docker compose ps
docker compose logs > logs.txt 2>&1
mvn spring-boot:run > app-logs.txt 2>&1 &
curl -s http://localhost:8080/actuator/health >> logs.txt
curl -s http://localhost:8081/subjects >> logs.txt
```

2. Check project GitHub Issues (if applicable).

3. Review Kafka docs:
   - https://kafka.apache.org/documentation/
   - https://docs.confluent.io/platform/current/installation/docker/overview.html

4. For Kafka Streams questions:
   - https://kafka.apache.org/documentation/streams/
