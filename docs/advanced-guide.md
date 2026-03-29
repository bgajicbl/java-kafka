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

### 1.2 Processor API Transformer: Fine-Grained Control

**What we're practicing:** The Processor API gives you **direct access to the processor context** and state stores, enabling sophisticated business logic.

**In this project:**

```java
events.transform(
  () -> new FraudDetectorTransformer(properties),  // Supplier creates new instance per task
  Named.as("fraud-detector"),
  Topics.FRAUD_ACTIVITY_STORE  // State store reference
)
```

**FraudDetectorTransformer:**
- Reads and updates `fraud-activity-store` directly inside `transform()`.
- Implements windowed burst detection (resets window on expiration).
- Returns `null` for non-fraudulent events (filters them out implicitly).

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
- Partitioning by tenant ensures co-location (same tenant events go to same partition).

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
1. No event is processed twice (idempotent offset management).
2. No event is skipped (offset committed only after state + output are durable).
3. All writes to output topics are **transactional** (atomic all-or-nothing).

**Why it matters for the role:**
- For financial systems, "exactly once" is **non-negotiable**. You cannot have duplicate payments or skipped transactions.
- EOS_V2 uses **transactional markers** in Kafka instead of external coordination, making it faster and simpler.
- Senior engineers understand **trade-offs:** EOS_V2 adds latency (~10-20ms) but ensures correctness.

---

## Part 2: Extension Points and Learning Exercises

### 2.1 Event Deduplication by ID

**Current state:** The project does NOT deduplicate events by `eventId`. Publishing the same event ID multiple times will process each as independent.

**Why:** This is intentional—you should implement it as an extension exercise.

**Implementation sketch:**

```java
// Add to config
KStream<String, TransactionEvent> dedupedEvents = events.transform(
  () -> new DeduplicationTransformer(stateDuration),
  Named.as("deduplication"),
  "dedup-store"
);

// DeduplicationTransformer logic:
// 1. On first eventId, emit event and record in state.
// 2. On duplicate eventId, suppress (return null).
// 3. After stateDuration, forget the eventId.
```

**Learning goal:** Understand Processor API state store usage and supplier pattern.

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

**Extension:** Add a quota-checking transformer:

```java
events.transform(
  () -> new RateLimitingTransformer(quotaStore),
  "quota-store"
)
.filterNot((k, v) -> v == null) // Drop over-quota events
```

**Learning goal:** Resource governance, backpressure, and fair multi-tenant allocation.

---

## Part 3: Multi-Instance Interactive Query Routing

**Problem:** Current IQ only works on the local instance. In production, you have multiple instances.

**Solution options:**

### 3.1 Metadata-Aware Routing (Recommended for learning)

```java
@GetMapping("/{tenantId}/{accountId}")
public BalanceView getBalance(@PathVariable String tenantId, @PathVariable String accountId) {
  String key = tenantId + "|" + accountId;
  
  // Get partition for key
  int partition = getPartition(key);
  
  // Get instance responsible for partition
  HostInfo hostInfo = getHost(partition);
  
  // If local, query local store
  if (isLocal(hostInfo)) {
    return queryLocalStore(key);
  }
  
  // Otherwise, redirect to remote instance
  return restTemplate.getForObject(
    "http://" + hostInfo.host() + ":" + hostInfo.port() 
    + "/api/ledger/" + tenantId + "/" + accountId,
    BalanceView.class
  );
}
```

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
watch 'docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group kafka-ledger-learning-v1 --describe'

# Kill an instance
pkill -f "spring-boot:run"

# Observe rebalance and rebuild time
# Good target: <30 seconds for 100k account state
```

### 5.2 Exactly-Once During Rebalance

- EOS_V2 uses **transactional markers** to ensure no duplicate output even during rebalance.
- If an instance crashes mid-transaction, Kafka rolls back uncommitted data.

**Extension:** Add monitoring for `transaction-abort-rate` to detect failure scenarios.

---

## Part 6: Cloud Deployment Patterns

### 6.1 Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ledger-app
spec:
  replicas: 3  # 3 instances for balance
  selector:
    matchLabels:
      app: ledger
  template:
    spec:
      containers:
      - name: app
        image: ledger-learning:latest
        ports:
        - containerPort: 8080
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: kafka-config
              key: bootstrap-servers
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 20
          periodSeconds: 5
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 2
            memory: 4Gi
```

**Learning points:**
- Liveness vs. readiness probes (when should traffic start?).
- Resource requests/limits for Streams apps (heavy on memory for state stores).
- Horizontal scaling and rebalancing coordination.

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
  --group kafka-ledger-learning-v1 \
  --describe

# Processing rate (events/second)
curl -s http://localhost:8080/actuator/metrics/kafka.stream.task.produced.records.total | jq

# State store size (bytes)
curl -s http://localhost:8080/actuator/metrics/kafka.stream.rocksdb.cache.pinned.usage | jq
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
2. **Implement 2-3 extensions** from **Part 2** (Dedup, Floor Enforcement, Windowing).
3. **Set up multi-instance routing** from **Part 3**.
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

