# Advanced Implementation and Architecture Guide

This guide digs deeper into the design decisions and extension points for senior engineers.

## Part 1: Design Patterns Explained

### 1.1 Stateful Stream Processing: State Stores & Interactive Queries

**What we're practicing:** Kafka Streams maintains local state stores (backed by RocksDB) to support stateful aggregations and low-latency reads.

**In this project:**

```
TransactionEvent ---> [Ledger Aggregation] --> State Store (ledger-balance-store)
                                                        |
                                                   Interactive Query API
                                                   (ReadOnlyKeyValueStore)
```

**Key concepts:**
- **Materialized View:** The `balance` topic is a compacted changelog of balances. IQ service queries the local store directly.
- **Changelog Topic:** Kafka Streams auto-creates `ledger-balance-store-changelog` to track store mutations for recovery.
- **Interactive Query Limitations:** By default, queries hit only the local instance. For distributed IQ, see **Part 3: Multi-Instance Scaling**.

**Why it matters for the role:**
- Senior engineers architect **exactly what is queryable and how fast**. This project shows you RocksDB-backed sub-millisecond queries.
- Understanding state store behavior is critical for financial systems (e.g., "can we serve account balances from Kafka Streams or do we need Cassandra?").

---

### 1.2 Processor API: Fine-Grained Control

**What we're practicing:** The Processor API gives you **direct access to the processor context** and state stores, enabling sophisticated business logic.

**In this project** (modern `FixedKeyProcessor` API via `processValues`; the deprecated `Transformer` API is not used):

```java
accepted.processValues(
  () -> new FraudDetectionProcessor(properties),  // Supplier creates new instance per task
  Named.as("fraud-detector"),
  Topics.FRAUD_ACTIVITY_STORE, Topics.FRAUD_EXPIRY_INDEX_STORE  // State store references
)
```

**FraudDetectionProcessor:**
- Reads and updates `fraud-activity-store` directly inside `process()`.
- Implements windowed burst detection (resets window on expiration).
- Forwards a `FraudAlert` only when a rule fires; otherwise forwards nothing.
- Schedules a **stream-time** punctuator that evicts idle accounts using a secondary
  expiry-index store, so cleanup range-scans only expired entries instead of every account.

The same API powers `TransactionGuardProcessor`, which de-duplicates `eventId`s with a window
store and enforces single-currency accounts before the balance aggregation.

**Why it matters for the role:**
- The Processor API is where **Kafka Streams becomes a programming language** for event logic.
- Senior engineers know when DSL suffices vs. when Processor API is required (custom state management, complex branching, etc.).

---

### 1.3 Multi-Tenant Keying Strategy

**What we're practicing:** Ensuring tenant isolation at the stream topology level, not at runtime.

**In this project:**

```java
KStream<String, TransactionEvent> events = builder
  .stream(Topics.TRANSACTION_EVENTS, ...)
  .selectKey((key, value) -> value.getTenantId() + "|" + value.getAccountId());
```

**Design:**
- Every key is scoped: `tenantId|accountId`.
- All state stores use this key format.
- IDs are validated (`[A-Za-z0-9._-]{1,64}`) so no ID can contain the `|` delimiter and forge
  another tenant's key.
- Keys are partitioned on the **full** composite key. Partitioning by `tenantId` alone was
  rejected: a large tenant would pin one partition (a hot partition) and nothing needs
  cross-account ordering within a tenant.
- At the API, a JWT `tenant_id` claim must match the tenant in every read and write
  (`security/TenantAccess`); the tenant in a request body is never trusted on its own.

**Why it matters for the role:**
- Multi-tenant architecture is **core to SaaS** and **a hiring expectation** for this role.
- You must think about: data leakage, quota enforcement, hot tenants, and fair resource allocation.
- This project validates no cross-tenant state leakage.

---

### 1.4 Exactly-Once Semantics (EOS_V2)

**What we're practicing:** Transactional consistency using Kafka transactions + idempotent writes.

**Current config:**
```yaml
processing.guarantee: exactly_once_v2
```

**What EOS_V2 guarantees:**
1. No input record is processed twice (idempotent offset management).
2. No input record is skipped (offset committed only after state + output are durable).
3. All writes to output topics are **transactional** (atomic all-or-nothing).

**What it does not guarantee:** that the *same business event* arrives only once. A client that
retries a POST produces a second, distinct record. That is why the ledger also de-duplicates on
`eventId` (see 2.1).

**Why it matters for the role:**
- For financial systems, "exactly once" is **non-negotiable**. You cannot have duplicate payments or skipped transactions.
- EOS_V2 uses **transactional markers** in Kafka instead of external coordination, making it faster and simpler.
- Senior engineers understand **trade-offs:** EOS_V2 adds latency (~10-20ms) but ensures correctness.

---

## Part 2: Extension Points and Learning Exercises

### 2.1 Event Deduplication by ID

**Current state:** Implemented for the ledger. `TransactionGuardProcessor` remembers each
`eventId` (scoped to `tenantId|accountId`) in the `tx-dedup-store` window store for
`app.ledger.dedup-window` (default 7 days) and drops replays, counting them in
`ledger_transactions_duplicates_total`.

```java
KStream<String, TransactionEvent> events = builder.stream(Topics.TRANSACTION_EVENTS, ...)
    .selectKey((key, value) -> value.getTenantId() + "|" + value.getAccountId())
    .repartition(Repartitioned.with(stringSerde, transactionSerde).withName("tx-by-account"));

events.processValues(
    () -> new TransactionGuardProcessor(dedupWindow, metrics),
    Named.as("tx-guard"),
    Topics.TX_DEDUP_STORE, Topics.ACCOUNT_CURRENCY_STORE);
```

Points worth studying:
- **Why the explicit `repartition()`:** the guard must see every replay of an event on the same
  task; deduplicating before the repartition would only work if every producer keyed correctly.
- **Why a window store:** retention bounds state growth automatically; the window should cover
  the input topic's retention, since that is how far back a replay can come from.
- **Why per-account scoping:** tenants choose their own IDs, so the same `eventId` in two tenants
  is not a duplicate.

**Extension:** Payment commands are not yet idempotent — a retried `INITIATE` is rejected to
`payment-dlq` as an invalid transition instead of being acknowledged as a no-op. Add a
`correlationId`-based dedup step in front of `PaymentStateMachine`.

**Learning goal:** Processor API state stores, the co-partitioning requirement, and the
difference between transport-level (EOS) and business-level idempotency.

---

### 2.2 Balance Floor Enforcement (Overdraft Protection)

**Current state:** Balance can go negative.

**Extension:** Add validation to reject debits that would exceed a floor.

```java
// In aggregation adder logic:
if (proposedBalance < MIN_BALANCE) {
  emit alert to rejected-transactions topic
  return original balance (unchanged)
}
```

**Learning goal:** Financial correctness, side-channel outputs, and error handling in streaming.

---

### 2.3 Windowed Aggregations (Time Windows)

**Current state:** Burst detection uses a simple sliding window (resets if gap > threshold).

**Extension:** Add true time-based windowing:

```java
events.groupByKey()
  .windowedBy(TimeWindows.ofSizeAndGrace(
    Duration.ofMinutes(1),
    Duration.ofSeconds(10)
  ))
  .aggregate(...) // Count transactions per minute
```

**Learning goal:** Understand Kafka Streams windowing semantics, grace period handling, and downstream state cleanup.

---

### 2.4 Schema Evolution and Compatibility

**Current state:** Schemas are simple with defaults.

**Extension:** Add an optional field with default:

```json
{
  "name": "metadata",
  "type": ["null", "string"],
  "default": null
}
```

Then test:
1. Publish with old schema (no metadata field).
2. Publish with new schema (includes metadata).
3. Verify backward and forward compatibility.

**Learning goal:** Schema Registry compatibility modes, versioning strategy, and safe rollout patterns.

---

### 2.5 Rate Limiting / Tenant Quotas

**Current state:** No rate limiting per tenant.

**Extension:** Add a quota-checking processor (same pattern as `TransactionGuardProcessor`):

```java
events.processValues(
  () -> new RateLimitingProcessor(quotaProperties),
  Named.as("tenant-quota"),
  "quota-store"
)
.filter((k, verdict) -> verdict.accepted()) // route rejected events to a DLQ instead of dropping
```

**Learning goal:** Resource governance, backpressure, and fair multi-tenant allocation.

---

## Part 3: Multi-Instance Interactive Query Routing

**Problem:** Each instance only holds the store partitions assigned to it. With N instances, a
local-only lookup misses for roughly (N-1)/N of keys.

### 3.1 Metadata-Aware Routing (Implemented)

`InteractiveQueryRouter` implements this. Each instance advertises `application.server`, and
every lookup goes through:

```java
public <T> Optional<T> query(String storeName, String key,
                             Function<UriBuilder, URI> remotePath,
                             ParameterizedTypeReference<T> responseType,
                             Supplier<Optional<T>> local) {
    Optional<HostInfo> owner = remoteOwner(storeName, key);   // queryMetadataForKey(...).activeHost()
    if (owner.isEmpty()) {
        return local.get();                                   // this instance owns the key
    }
    // forward to the owner with X-Ledger-IQ-Forwarded + the caller's Authorization header
    ...
}
```

Details worth studying:
- **Loop prevention:** a forwarded request is always answered locally, so two instances with
  different views of the assignment during a rebalance cannot bounce a request between them.
- **Rebalances:** with no active host (or a store still restoring) the API returns `503` with
  `Retry-After`, not a `500`.
- **Security:** the caller's token is forwarded, so the owning instance re-applies the same
  tenant check.
- **Kubernetes:** `application.server` must be stable, so the app runs as a StatefulSet behind a
  headless service (see 6.1).

**Exercise:** run two instances locally on different ports with different `state.dir`s and
`APP_ADVERTISED_HOST=localhost`, then query keys owned by each through either port.

**Learning goal:** Understand Kafka partition assignment, metadata access, and distributed query routing.

### 3.2 Cache Layer (External State Store)

```
Kafka Streams State Store --> Sync to Redis
                                     ^
                                     |
Query API reads from Redis (with fallback to IQ)
```

**Pros:** Query any instance, no routing complexity.
**Cons:** Additional operational burden (Redis cluster), eventual consistency.

---

## Part 4: RocksDB Tuning for Financial Workloads

### 4.1 Cache vs. Commit Interval Trade-off

**Default config:**
```yaml
cache.max.bytes.buffering: 10485760  # 10 MB
commit.interval.ms: 100
```

**Scenario 1: High-throughput payment processing**
- Increase cache to 100-500 MB (assumes 64 GB machine).
- Reduce commit interval to 50-100ms (trade throughput for lower latency).

**Scenario 2: Low-latency reads (financial dashboard)**
- Smaller cache (20 MB), longer commit (1 second).
- Use RocksDB compression to reduce memory.

**Scenario 3: Large state (millions of accounts)**
- Explicit `RocksDBConfigSetter` with custom options:

```java
public class CustomRocksDBConfigSetter implements RocksDBConfigSetter {
  @Override
  public void setConfig(String storeName, Options options, Map<String, Object> configs) {
    options.setMaxWriteBufferNumber(4);
    options.setWriteBufferSize(128 * 1024 * 1024); // 128 MB per buffer
    // Enable compression
    options.setCompressionType(CompressionType.LZ4_COMPRESSION);
  }
}
```

**Learning goal:** Understand RocksDB internals, memory/latency/throughput trade-offs, and tuning for financial workloads.

---

## Part 5: Failure Scenarios and Resilience

### 5.1 Rebalancing Behavior

When a new instance joins or leaves:
1. Kafka triggers a rebalance (all instances stop processing temporarily).
2. State store changelog is **repartitioned**.
3. State is **rebuilt** from changelog topics.

**Measurement exercise:**

```bash
# Watch rebalance duration
watch 'docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group kafka-ledger-learning-v2 --describe'

# Kill an instance
pkill -f "spring-boot:run"

# Observe rebalance and rebuild time
# Good target: <30 seconds for 100k account state
```

### 5.2 Exactly-Once During Rebalance

- EOS_V2 uses **transactional markers** to ensure no duplicate output even during rebalance.
- If an instance crashes mid-transaction, Kafka rolls back uncommitted data.

**Extension:** Add monitoring for `transaction-abort-rate` to detect failure scenarios.

### 5.3 What Happens on a Bad Record

The project never drops a record silently (see `comprehensive-reference.md` §14):
- Undeserialisable input goes to `dlq-deserialization-errors`; if that write fails, the thread fails.
- A production error or an exception in a processor shuts the Streams client down
  (`SHUTDOWN_CLIENT`), the `kafkaStreams` health indicator turns liveness `DOWN`, and Kubernetes
  restarts the pod. A deterministic poison record crash-loops the pod on purpose.

**Exercise:** produce a non-Avro record to `tx-events`, inspect its DLQ headers, and confirm
processing continues. Then explain when you would prefer `CONTINUE` over `FAIL`.

---

## Part 6: Cloud Deployment Patterns

### 6.1 Kubernetes: StatefulSet, Not Deployment

See `k8s/base/statefulset.yaml`. The essential parts:

```yaml
apiVersion: apps/v1
kind: StatefulSet
spec:
  serviceName: kafka-ledger-headless      # stable per-pod DNS names
  podManagementPolicy: Parallel
  template:
    spec:
      containers:
        - name: kafka-ledger
          env:
            - name: APP_ADVERTISED_HOST     # → application.server for IQ routing
              value: "$(POD_NAME).kafka-ledger-headless.$(POD_NAMESPACE).svc.cluster.local"
          volumeMounts:
            - name: state-store
              mountPath: /var/kafka-streams
  volumeClaimTemplates:                     # RocksDB survives pod restarts
    - metadata:
        name: state-store
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
```

**Learning points:**
- **Why StatefulSet:** stable identity for `application.server`, and a persistent volume per pod
  so a restart resumes from local RocksDB instead of replaying every changelog.
- **Standby replicas** (`num.standby.replicas: 1` in the mounted `application-k8s.yml`) keep a
  warm copy of each store on another pod for fast failover.
- **Probes:** liveness and readiness both include the `kafkaStreams` health indicator; a client in
  `ERROR` never recovers by itself, so the pod must restart.
- **Autoscaling:** the HPA caps replicas at the input partition count, scales on CPU only, and
  moves one pod at a time with long stabilisation windows — every scale event is a rebalance.

### 6.2 Terraform Infrastructure as Code

```hcl
# main.tf
resource "aws_msk_cluster" "kafka" {
  cluster_name           = "ledger-kafka"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3
  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    ebs_volume_size = 100
  }
}

resource "aws_eks_cluster" "app" {
  name    = "ledger-app"
  version = "1.27"
  # ... configuration
}

resource "aws_eks_node_group" "app_nodes" {
  node_group_name = "app-nodes"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  desired_size    = 3
  min_size        = 2
  max_size        = 10
  instance_types  = ["t3.xlarge"]
}
```

**Learning goal:** Understand infrastructure-as-code, cloud resource provisioning, and GitOps workflows.

---

## Part 7: Operational Observability

### 7.1 Key Metrics for Ledger System

```bash
# Consumer lag (how far behind are we?)
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group kafka-ledger-learning-v2 \
  --describe

# Processing rate (events/second)
curl -s http://localhost:8080/actuator/metrics/kafka.stream.task.produced.records.total | jq

# State store size (bytes)
curl -s http://localhost:8080/actuator/metrics/kafka.stream.rocksdb.cache.pinned.usage | jq

# Business counters
curl -s http://localhost:8080/actuator/prometheus \
  | grep -E "ledger_transactions_(processed|duplicates)|payments_state_transitions|fraud_alerts_emitted|dlq_records"
```

### 7.2 Alerting Rules

```
Alert: HighConsumerLag
if consumer_lag > 1000 for 5 minutes
action: page on-call engineer

Alert: StateRestoreLag
if time_since_state_restored > 60s
action: investigate rebalance issues

Alert: TxnAbortRate
if transaction_abort_rate > 0.001
action: investigate EOS compliance, check broker health

Alert: DeadLetters
if increase(dlq_records_total[10m]) > 0
action: triage the DLQ topic named in the `topic` tag

Alert: StreamsClientDown
if kafkaStreams health component is DOWN (pod restarting / crash-looping)
action: read the failing record's coordinates from the logs before it is retried
```

---

## Part 8: Interview Prep - Technical Questions You Can Answer Now

After completing this guide and the labs, you should be able to answer:

1. **"How would you ensure account balances are consistent across replicas?"**
   - Answer: Compacted `ledger-balances` topic + exactly-once semantics + state store changelog.

2. **"Can you scale to millions of accounts?"**
   - Answer: Yes. Partitioning by `tenantId|accountId` allows horizontal scaling. RocksDB can handle billions of KV pairs per instance. Multi-instance IQ routing via metadata is the pattern.

3. **"What happens if Kafka broker crashes?"**
   - Answer: EOS_V2 rolls back any in-flight transactions. On restart, state stores rebuild from changelog topics. Application pauses during rebalance but no data is lost.

4. **"How would you test this in production before cutover?"**
   - Answer: Shadow mode (produce to both old and new systems, compare outputs), gradual canary roll (5% → 25% → 100% of traffic).

5. **"What would you monitor in production?"**
   - Answer: Consumer lag, end-to-end latency p95/p99, state store size, transaction abort rate, error rates by exception type.

---

## Summary and Next Steps

1. **Complete Labs 1-6** in `testing-playbook.md` (Weeks 1-2).
2. **Implement 2-3 extensions** from **Part 2** (payment-command idempotency, Floor Enforcement, Windowing, Quotas).
3. **Run two instances locally** and exercise the IQ routing from **Part 3**.
4. **Measure RocksDB performance** using **Part 4** tuning guide.
5. **Document an ADR** for your design choices (topology, state, keying, EOS mode).
6. **Containerize and deploy to K8s** using **Part 6** templates.
7. **Set up observability** (Prometheus/Grafana) using **Part 7** metrics.

By the end, you'll have:
- ✅ Hands-on Kafka Streams expertise (DSL + Processor API)
- ✅ Multi-tenant SaaS design patterns
- ✅ Financial data integrity best practices
- ✅ Production operations readiness
- ✅ Cloud deployment and IaC experience

This directly addresses the job description's requirements for 4+ years of Kafka Streams production experience and multi-tenant architecture expertise.

